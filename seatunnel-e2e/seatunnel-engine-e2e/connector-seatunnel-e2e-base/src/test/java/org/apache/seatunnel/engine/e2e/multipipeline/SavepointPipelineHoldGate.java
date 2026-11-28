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

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only synchronization point that keeps a bounded BATCH source from signalling the end of its
 * input until the test releases it.
 *
 * <p>The Zeta cluster used by the engine E2E tests runs entirely inside the test JVM, so a static
 * registry keyed by a per-test hold key is enough to coordinate the test thread with the task
 * thread that owns the reader. A reader that already emitted every row of its splits but withholds
 * {@code signalNoMoreElement()} keeps its pipeline {@code RUNNING} without any timing assumption
 * about how long a finite read delay lasts: the pipeline cannot finish on its own until the test
 * releases the gate. Because the reader waits outside the {@code checkpointLock}, a checkpoint or
 * savepoint barrier can still be injected and acknowledged while the pipeline is held.
 *
 * <p>{@link #release(String)} lets the held readers finish their input, while {@link
 * #clear(String)} aborts the hold for teardown: a reader released by teardown must not report the
 * end of its input to a job that is already being stopped. An interrupt ends the hold as well,
 * because it is delivered while the job that owns the pipeline is stopping. A hard deadline
 * additionally guarantees that a broken test can never hold a task thread forever.
 */
@Slf4j
public final class SavepointPipelineHoldGate {

    /** Upper bound for a single hold so that a broken test cannot block a task thread forever. */
    private static final long MAX_HOLD_MILLIS = TimeUnit.MINUTES.toMillis(5);

    /** Armed holds keyed by hold key; a missing entry means "never hold". */
    private static final Map<String, Hold> HOLDS = new ConcurrentHashMap<>();

    private SavepointPipelineHoldGate() {}

    /** One armed hold shared by the test thread and the reader threads of the pipeline. */
    private static final class Hold {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicInteger holdingReaders = new AtomicInteger();
        /** Whether a released reader may report the end of its input. */
        private volatile boolean finishInputAllowed = true;
    }

    /**
     * Arms the gate: every reader that reaches the end of its input with this key is held until
     * {@link #release(String)}. Arming again after a release installs a fresh hold, which is what
     * the restore half of the test uses to observe the restored pipeline running.
     *
     * @param holdKey unique key shared between the test and the source configuration
     */
    public static void arm(String holdKey) {
        HOLDS.put(holdKey, new Hold());
    }

    /**
     * Releases every reader currently held for the key so it finishes its input, and lets later
     * readers of the same hold pass through immediately. Safe to call repeatedly and for keys that
     * were never armed.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void release(String holdKey) {
        Hold hold = HOLDS.get(holdKey);
        if (hold != null) {
            hold.finishInputAllowed = true;
            hold.latch.countDown();
        }
    }

    /**
     * Aborts the hold and forgets the key: held readers return without reporting the end of their
     * input, which is what teardown needs once the job is already being stopped. Intended for test
     * teardown so that a rerun with the same key starts from a clean registry.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     */
    public static void clear(String holdKey) {
        Hold hold = HOLDS.remove(holdKey);
        if (hold == null) {
            return;
        }
        hold.finishInputAllowed = false;
        hold.latch.countDown();
    }

    /**
     * Returns how many readers are held for the key right now, i.e. how many pipelines the test
     * currently keeps active on purpose.
     *
     * @param holdKey the key passed to {@link #arm(String)}
     * @return number of held readers, zero when the key is unknown
     */
    public static int holdingReaders(String holdKey) {
        Hold hold = HOLDS.get(holdKey);
        return hold == null ? 0 : hold.holdingReaders.get();
    }

    /**
     * Blocks the calling reader until the key is released, aborted or the hold deadline passes.
     *
     * <p>Returns immediately when the key was never armed or is no longer held, so a reader that is
     * not meant to be held can always finish its input.
     *
     * @param holdKey the key configured on the source
     * @return whether the reader may still report the end of its input
     * @throws InterruptedException when the job that owns the reader is being stopped
     */
    static boolean awaitRelease(String holdKey) throws InterruptedException {
        Hold hold = HOLDS.get(holdKey);
        if (hold == null) {
            return true;
        }
        hold.holdingReaders.incrementAndGet();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_HOLD_MILLIS);
        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    log.warn(
                            "Hold key {} was not released within {} ms, letting the reader finish",
                            holdKey,
                            MAX_HOLD_MILLIS);
                    return true;
                }
                if (hold.latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                    return hold.finishInputAllowed;
                }
            }
        } finally {
            hold.holdingReaders.decrementAndGet();
        }
    }
}
