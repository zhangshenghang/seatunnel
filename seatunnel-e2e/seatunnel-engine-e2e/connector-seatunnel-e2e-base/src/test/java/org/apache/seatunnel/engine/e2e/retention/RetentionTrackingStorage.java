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

package org.apache.seatunnel.engine.e2e.retention;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Real local-file checkpoint storage that records every successful write and every delete request.
 *
 * <p>The coordinator's retention policy is expressed as storage side effects: when it rotates, it
 * asks storage to delete a block of checkpoint ids. This fixture keeps the {@code (pipeline,
 * checkpoint id)} pairs the engine actually persisted and the delete requests it issued, so the E2E
 * can prove that retention only ever deletes checkpoints that the same pipeline had already stored
 * successfully.
 */
public class RetentionTrackingStorage extends LocalFileStorage {

    private static final Map<String, List<Long>> STORED_IDS = new ConcurrentHashMap<>();

    private static final List<DeleteRequest> DELETE_REQUESTS = new CopyOnWriteArrayList<>();

    private static final AtomicInteger JOB_LEVEL_DELETES = new AtomicInteger();

    /** The fence the test currently holds, or {@code null} when checkpointing may proceed. */
    private static final AtomicReference<CheckpointFence> ACTIVE_FENCE = new AtomicReference<>();

    /**
     * Upper bound for how long a checkpoint thread may stay parked in a fence. The test only holds
     * a fence long enough to read the frozen ledger, so this bound never fires in a passing run; it
     * exists so that a test failure cannot leave the job's checkpoint threads blocked forever.
     */
    private static final long FENCE_PARK_TIMEOUT_MILLIS = 20_000L;

    private static final long FENCE_POLL_MILLIS = 20L;

    public RetentionTrackingStorage(Map<String, String> configuration) {
        super(configuration);
    }

    /** Clears the static bookkeeping before a test starts. */
    public static void reset() {
        STORED_IDS.clear();
        DELETE_REQUESTS.clear();
        JOB_LEVEL_DELETES.set(0);
        ACTIVE_FENCE.set(null);
    }

    public static List<Long> storedIds(String jobId, int pipelineId) {
        return STORED_IDS.getOrDefault(key(jobId, pipelineId), Collections.emptyList());
    }

    public static List<DeleteRequest> deleteRequests() {
        return DELETE_REQUESTS;
    }

    public static List<DeleteRequest> deleteRequestsOf(int pipelineId) {
        List<DeleteRequest> requests = new CopyOnWriteArrayList<>();
        for (DeleteRequest request : DELETE_REQUESTS) {
            if (request.getPipelineId() == pipelineId) {
                requests.add(request);
            }
        }
        return requests;
    }

    public static int jobLevelDeletes() {
        return JOB_LEVEL_DELETES.get();
    }

    /**
     * Arms a fence that parks one pipeline before its first retention rotation and the other after
     * it, so the test can inspect a stable, coherent ledger while overlapping rotations of one
     * pipeline have already happened.
     *
     * <p>The first pipeline that tries to store the {@code anchorHold + 1}-th checkpoint becomes
     * the anchor and stops completing checkpoints exactly at {@code anchorHold}, before it rotates
     * anything. The other pipeline keeps checkpointing until it is about to store the {@code
     * rotatorHold + 1}-th checkpoint, i.e. after its first full retention rotation, and parks
     * there.
     *
     * @param anchorHold completed checkpoints the anchor pipeline is held at
     * @param rotatorHold completed checkpoints the rotating pipeline is held at
     */
    public static void armAsymmetricFence(long anchorHold, long rotatorHold) {
        ACTIVE_FENCE.set(CheckpointFence.asymmetric(anchorHold, rotatorHold));
    }

    /**
     * Arms a fence that parks the given pipelines before their next checkpoint store, as a common
     * observation boundary for a symmetric assertion.
     *
     * @param pipelineIds pipelines that must park before their next store
     */
    public static void armFenceForPipelines(Collection<Integer> pipelineIds) {
        ACTIVE_FENCE.set(CheckpointFence.forPipelines(new HashSet<>(pipelineIds)));
    }

    /**
     * Waits until at least {@code expectedPipelines} pipelines are parked in the current fence.
     *
     * @return {@code true} if all expected pipelines parked before the timeout
     */
    public static boolean awaitFenceHeld(int expectedPipelines, long timeout, TimeUnit unit)
            throws InterruptedException {
        CheckpointFence fence = ACTIVE_FENCE.get();
        return fence != null && fence.awaitAllParked(expectedPipelines, timeout, unit);
    }

    /** Releases the current fence, if any, and lets parked checkpoint threads continue. */
    public static void releaseFence() {
        CheckpointFence fence = ACTIVE_FENCE.getAndSet(null);
        if (fence != null) {
            fence.release();
        }
    }

    private static int storedCount(String jobId, int pipelineId) {
        return storedIds(jobId, pipelineId).size();
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        CheckpointFence fence = ACTIVE_FENCE.get();
        if (fence != null
                && fence.shouldPark(
                        state.getPipelineId(),
                        storedCount(state.getJobId(), state.getPipelineId()))) {
            fence.park(state.getPipelineId());
        }
        String location = super.storeCheckPoint(state);
        STORED_IDS
                .computeIfAbsent(
                        key(state.getJobId(), state.getPipelineId()),
                        ignored -> new CopyOnWriteArrayList<>())
                .add(state.getCheckpointId());
        return location;
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        DELETE_REQUESTS.add(
                new DeleteRequest(
                        Integer.parseInt(pipelineId),
                        Collections.singletonList(Long.parseLong(checkpointId))));
        super.deleteCheckpoint(jobId, pipelineId, checkpointId);
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIdList)
            throws CheckpointStorageException {
        List<Long> ids = new CopyOnWriteArrayList<>();
        for (String checkpointId : checkpointIdList) {
            ids.add(Long.parseLong(checkpointId));
        }
        DELETE_REQUESTS.add(new DeleteRequest(Integer.parseInt(pipelineId), ids));
        super.deleteCheckpoint(jobId, pipelineId, checkpointIdList);
    }

    @Override
    public void deleteCheckpoint(String jobId) {
        JOB_LEVEL_DELETES.incrementAndGet();
        super.deleteCheckpoint(jobId);
    }

    private static String key(String jobId, int pipelineId) {
        return jobId + ":" + pipelineId;
    }

    /**
     * Test-only gate that parks a coordinator before it stores the next checkpoint, which makes the
     * tracking ledger, the files on disk and the checkpoint monitor one coherent observation.
     *
     * <p>The coordinator finalizes a checkpoint in a single serialized method: it stores the state,
     * applies retention and only then publishes the completion to the monitor. Parking before the
     * <em>next</em> store therefore guarantees that a parked pipeline has no file write, no
     * retention delete and no monitor update in flight, and that every completion the monitor
     * reports is already fully stored. That is what a read-only snapshot of live storage cannot
     * give the test while the 500 ms scheduler keeps completing checkpoints.
     */
    private static final class CheckpointFence {

        private final long anchorHold;
        private final long rotatorHold;
        private final Set<Integer> pipelineIds;
        private final AtomicBoolean anchorClaimed = new AtomicBoolean();
        private final Set<Integer> parked = ConcurrentHashMap.newKeySet();
        private final CountDownLatch released = new CountDownLatch(1);
        private volatile boolean disarmed;

        private CheckpointFence(long anchorHold, long rotatorHold, Set<Integer> pipelineIds) {
            this.anchorHold = anchorHold;
            this.rotatorHold = rotatorHold;
            this.pipelineIds = pipelineIds;
        }

        static CheckpointFence asymmetric(long anchorHold, long rotatorHold) {
            return new CheckpointFence(anchorHold, rotatorHold, null);
        }

        static CheckpointFence forPipelines(Set<Integer> pipelineIds) {
            return new CheckpointFence(-1L, -1L, pipelineIds);
        }

        boolean shouldPark(int pipelineId, int storedCheckpoints) {
            if (disarmed) {
                return false;
            }
            if (pipelineIds != null) {
                return pipelineIds.contains(pipelineId);
            }
            if (storedCheckpoints >= rotatorHold) {
                return true;
            }
            return storedCheckpoints >= anchorHold && anchorClaimed.compareAndSet(false, true);
        }

        void park(int pipelineId) {
            parked.add(pipelineId);
            try {
                released.await(FENCE_PARK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        boolean awaitAllParked(int expected, long timeout, TimeUnit unit)
                throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            while (parked.size() < expected && System.nanoTime() < deadline) {
                Thread.sleep(FENCE_POLL_MILLIS);
            }
            return parked.size() >= expected;
        }

        void release() {
            disarmed = true;
            released.countDown();
        }
    }

    /** One call the coordinator made to delete checkpoints of a single pipeline. */
    public static final class DeleteRequest {

        private final int pipelineId;
        private final List<Long> checkpointIds;

        DeleteRequest(int pipelineId, List<Long> checkpointIds) {
            this.pipelineId = pipelineId;
            this.checkpointIds = checkpointIds;
        }

        public int getPipelineId() {
            return pipelineId;
        }

        public List<Long> getCheckpointIds() {
            return checkpointIds;
        }

        @Override
        public String toString() {
            return "DeleteRequest{pipelineId="
                    + pipelineId
                    + ", checkpointIds="
                    + checkpointIds
                    + '}';
        }
    }
}
