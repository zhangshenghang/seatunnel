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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorageFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.seatunnel.engine.common.Constant.IMAP_RUNNING_JOB_STATE;

@DisabledOnOs(OS.WINDOWS)
public class CheckpointManagerTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testFinishedPipelineShouldCleanupCounterAndCheckpointState() throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);
        CounterStateStore<String> checkpointCounterStore =
                server.getEngineContext().getStateStores().checkpointCounterStore();
        String counterKey = StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1);
        checkpointCounterStore.set(counterKey, 2L);
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, new CheckpointConfig());

        Assertions.assertEquals(
                2L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
        checkpointManager.listenPipeline(1, PipelineStatus.FINISHED).join();
        Assertions.assertNull(checkpointCounterStore.get(counterKey));
        checkpointManager.clearCheckpointIfNeed(JobStatus.FINISHED);
        Assertions.assertTrue(checkpointStorage.getAllCheckpoints(jobId + "").isEmpty());
    }

    @Test
    public void testStartWithSavepointShouldResumeNextCheckpointIdFromStoredCheckpoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        long restoredCheckpointId = 7L;
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(
                checkpointStorage, jobId, restoredCheckpointId, CheckpointType.SAVEPOINT_TYPE);

        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        jobId,
                        checkpointStorage,
                        new CheckpointConfig());

        long nextCheckpointId =
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get();

        Assertions.assertEquals(restoredCheckpointId + 1L, nextCheckpointId);
        Assertions.assertEquals(
                restoredCheckpointId + 1L,
                checkpointManager
                        .getCheckpointCoordinator(1)
                        .getCheckpointIdCounter()
                        .getAndIncrement());
        Assertions.assertEquals(
                restoredCheckpointId + 2L,
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testStartWithCheckpointShouldResumeNextCheckpointIdFromStoredCheckpoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        long restoredCheckpointId = 7L;
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(
                checkpointStorage,
                jobId,
                restoredCheckpointId,
                CheckpointType.COMPLETED_POINT_TYPE);

        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.CHECKPOINT,
                        jobId,
                        checkpointStorage,
                        new CheckpointConfig());

        long nextCheckpointId =
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get();

        Assertions.assertEquals(restoredCheckpointId + 1L, nextCheckpointId);
        Assertions.assertEquals(
                restoredCheckpointId + 1L,
                checkpointManager
                        .getCheckpointCoordinator(1)
                        .getCheckpointIdCounter()
                        .getAndIncrement());
        Assertions.assertEquals(
                restoredCheckpointId + 2L,
                checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testMasterFailoverRestoreShouldLoadLatestCheckpointOfOwnJob() throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 7L);

        // A normally submitted job has no restore source; after a master switch it is
        // re-initialized with restart = true and must still continue from its own checkpoint.
        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.NONE,
                        null,
                        true,
                        checkpointStorage,
                        new CheckpointConfig());

        Assertions.assertEquals(
                8L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testMasterFailoverRestoreShouldPreferOwnLatestCheckpointOverSavepoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 3L, CheckpointType.SAVEPOINT_TYPE);
        // checkpoint files are ordered by their store timestamp (millis)
        Thread.sleep(5);
        storeCompletedCheckpoint(checkpointStorage, jobId, 7L);

        // Job started with -r and later hit a master switch: resume from checkpoint 7, not
        // from the savepoint it was originally restored from.
        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        jobId,
                        true,
                        checkpointStorage,
                        new CheckpointConfig());

        Assertions.assertEquals(
                8L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    @Test
    public void testMasterFailoverRestoreShouldFallBackToSourceJobWithoutOwnCheckpoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        long sourceJobId = jobId + 1;
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, sourceJobId, 5L);

        CheckpointManager checkpointManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.CHECKPOINT,
                        sourceJobId,
                        true,
                        checkpointStorage,
                        new CheckpointConfig());

        Assertions.assertEquals(
                6L, checkpointManager.getCheckpointCoordinator(1).getCheckpointIdCounter().get());
    }

    private CheckpointStorage createCheckpointStorage() throws CheckpointStorageException {
        return FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CheckpointStorageFactory.class,
                        new CheckpointStorageConfig().getStorage())
                .create(new HashMap<>());
    }

    private void storeCompletedCheckpoint(
            CheckpointStorage checkpointStorage, long jobId, long checkpointId)
            throws CheckpointStorageException {
        storeCompletedCheckpoint(
                checkpointStorage, jobId, checkpointId, CheckpointType.COMPLETED_POINT_TYPE);
    }

    private void storeCompletedCheckpoint(
            CheckpointStorage checkpointStorage,
            long jobId,
            long checkpointId,
            CheckpointType checkpointType)
            throws CheckpointStorageException {
        storeCompletedCheckpoint(checkpointStorage, jobId, 1, checkpointId, checkpointType);
    }

    private void storeCompletedCheckpoint(
            CheckpointStorage checkpointStorage,
            long jobId,
            int pipelineId,
            long checkpointId,
            CheckpointType checkpointType)
            throws CheckpointStorageException {
        CompletedCheckpoint completedCheckpoint =
                new CompletedCheckpoint(
                        jobId,
                        pipelineId,
                        checkpointId,
                        Instant.now().toEpochMilli(),
                        checkpointType,
                        Instant.now().toEpochMilli(),
                        new HashMap<>(),
                        new HashMap<>());
        checkpointStorage.storeCheckPoint(
                PipelineState.builder()
                        .jobId(jobId + "")
                        .pipelineId(pipelineId)
                        .checkpointId(checkpointId)
                        .states(new ProtoStuffSerializer().serialize(completedCheckpoint))
                        .build());
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isStartWithSavePoint,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig) {
        return createCheckpointManager(
                jobId,
                isStartWithSavePoint,
                isStartWithSavePoint ? RestoreMode.SAVEPOINT : RestoreMode.NONE,
                isStartWithSavePoint ? jobId : null,
                checkpointStorage,
                checkpointConfig);
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isStartWithSavePoint,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig,
            int pipelineCount) {
        return createCheckpointManager(
                jobId,
                isStartWithSavePoint,
                isStartWithSavePoint ? RestoreMode.SAVEPOINT : RestoreMode.NONE,
                isStartWithSavePoint ? jobId : null,
                false,
                checkpointStorage,
                checkpointConfig,
                pipelineCount);
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig) {
        return createCheckpointManager(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                false,
                checkpointStorage,
                checkpointConfig);
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            boolean masterFailoverRestore,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig) {
        return createCheckpointManager(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                masterFailoverRestore,
                checkpointStorage,
                checkpointConfig,
                1);
    }

    private CheckpointManager createCheckpointManager(
            long jobId,
            boolean isRestoreJob,
            RestoreMode restoreMode,
            Long restoreSourceJobId,
            boolean masterFailoverRestore,
            CheckpointStorage checkpointStorage,
            CheckpointConfig checkpointConfig,
            int pipelineCount) {
        Map<Integer, CheckpointPlan> planMap = new HashMap<>();
        for (int pipelineId = 1; pipelineId <= pipelineCount; pipelineId++) {
            planMap.put(pipelineId, CheckpointPlan.builder().pipelineId(pipelineId).build());
        }
        return new CheckpointManager(
                jobId,
                isRestoreJob,
                restoreMode,
                restoreSourceJobId,
                masterFailoverRestore,
                nodeEngine,
                null,
                planMap,
                checkpointConfig,
                checkpointStorage,
                instance.getExecutorService("test"),
                nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE),
                server.getEngineContext(),
                null);
    }

    @Test
    public void testRetainCheckpointAfterCancelledWhenEnabled() throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);

        CheckpointConfig config = new CheckpointConfig();
        config.setRetainAfterJobCancelled(true);
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, config);

        // Checkpoint should be retained on CANCELED when retainAfterJobCancelled=true
        checkpointManager.clearCheckpointIfNeed(JobStatus.CANCELED);
        Assertions.assertFalse(
                checkpointStorage.getAllCheckpoints(jobId + "").isEmpty(),
                "Checkpoint should be retained after cancel when retain-after-job-cancelled is enabled");
    }

    @Test
    public void testDeleteCheckpointAfterFinishedEvenWhenRetainEnabled()
            throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);

        CheckpointConfig config = new CheckpointConfig();
        config.setRetainAfterJobCancelled(true);
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, config);

        // Checkpoint should still be deleted on FINISHED even when retainAfterJobCancelled=true
        checkpointManager.clearCheckpointIfNeed(JobStatus.FINISHED);
        Assertions.assertTrue(
                checkpointStorage.getAllCheckpoints(jobId + "").isEmpty(),
                "Checkpoint should be cleaned up after FINISHED regardless of retain setting");
    }

    @Test
    public void testDeleteCheckpointAfterCancelledWhenRetainDisabled()
            throws CheckpointStorageException {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1L);

        CheckpointConfig config = new CheckpointConfig();
        // retainAfterJobCancelled defaults to false
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, config);

        // Default behavior: checkpoint should be deleted on CANCELED
        checkpointManager.clearCheckpointIfNeed(JobStatus.CANCELED);
        Assertions.assertTrue(
                checkpointStorage.getAllCheckpoints(jobId + "").isEmpty(),
                "Checkpoint should be cleaned up after cancel when retain is disabled (default)");
    }

    /**
     * The savepoint fan-out may only skip a pipeline that ended <em>without error</em>. The
     * selection is exercised on real coordinators: one that completed its final checkpoint, one
     * that was cancelled and one that is still running. A failed or cancelled pipeline also ended,
     * but it must still take part so the request is reported as failed instead of silently
     * succeeding, and only the cleanly completed pipeline is recorded durably as skipped.
     */
    @Test
    public void testSavepointFanOutSkipsOnlyCleanlyCompletedPipelines() throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, new CheckpointConfig(), 3);
        checkpointManager
                .getCheckpointCoordinator(1)
                .completePendingCheckpoint(
                        completedCheckpoint(jobId, 1, 3L, CheckpointType.COMPLETED_POINT_TYPE));
        checkpointManager.getCheckpointCoordinator(2).cancelCheckpoint().join();

        Map<Integer, CheckpointCoordinator> selected =
                checkpointManager.selectSavepointCoordinators();

        Assertions.assertEquals(
                new HashSet<>(Arrays.asList(2, 3)),
                selected.keySet(),
                "Only a cleanly completed pipeline may be skipped; a cancelled or running pipeline "
                        + "must still make the savepoint fail");
        Assertions.assertTrue(
                checkpointManager.getCheckpointCoordinator(1).isCleanlyCompleted(),
                "a pipeline that finished its final checkpoint cleanly is completed");
        Assertions.assertFalse(
                checkpointManager.getCheckpointCoordinator(2).isCleanlyCompleted(),
                "a cancelled pipeline is not cleanly completed");
        Assertions.assertFalse(
                checkpointManager.getCheckpointCoordinator(3).isCleanlyCompleted(),
                "a running pipeline is not cleanly completed");

        CompletedCheckpoint marker = readLatestCheckpoint(checkpointStorage, jobId, 1);
        Assertions.assertNotNull(
                marker, "the skipped pipeline must be recorded durably with its final checkpoint");
        Assertions.assertEquals(
                CheckpointType.COMPLETED_POINT_TYPE,
                marker.getCheckpointType(),
                "the durable completion marker is the pipeline's completed-point checkpoint");
        Assertions.assertNull(
                readLatestCheckpoint(checkpointStorage, jobId, 2),
                "a cancelled pipeline has no completion marker");
        Assertions.assertNull(
                readLatestCheckpoint(checkpointStorage, jobId, 3),
                "a running pipeline has no completion marker");

        long allCompletedJobId = jobId + 1;
        CheckpointManager allCompletedManager =
                createCheckpointManager(
                        allCompletedJobId, false, checkpointStorage, new CheckpointConfig(), 1);
        allCompletedManager
                .getCheckpointCoordinator(1)
                .completePendingCheckpoint(
                        completedCheckpoint(
                                allCompletedJobId, 1, 3L, CheckpointType.COMPLETED_POINT_TYPE));
        Assertions.assertTrue(
                allCompletedManager.selectSavepointCoordinators().isEmpty(),
                "A request where every pipeline already completed cleanly has no participant");
    }

    /**
     * The completion marker a savepoint writes for a skipped pipeline is what explains the missing
     * savepoint state of that pipeline on restore: the writer persists the final completed-point
     * checkpoint that the regular finalization deliberately does not store, and the restore then
     * classifies the pipeline as finished instead of running it again.
     */
    @Test
    public void testSavepointRestoreSkipsPipelineThatCompletedBeforeTheSavepoint()
            throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        CheckpointManager checkpointManager =
                createCheckpointManager(jobId, false, checkpointStorage, new CheckpointConfig(), 2);
        // Pipeline 1 finished cleanly before the savepoint: finalization keeps its completed-point
        // checkpoint in memory only.
        checkpointManager
                .getCheckpointCoordinator(1)
                .completePendingCheckpoint(
                        completedCheckpoint(jobId, 1, 4L, CheckpointType.COMPLETED_POINT_TYPE));
        Assertions.assertNull(
                readLatestCheckpoint(checkpointStorage, jobId, 1),
                "the regular finalization must not store the completed-point checkpoint");

        // The savepoint skips pipeline 1 and records that durably; pipeline 2 stays active.
        Assertions.assertEquals(
                new HashSet<>(Collections.singletonList(2)),
                checkpointManager.selectSavepointCoordinators().keySet(),
                "only the finished pipeline is skipped by the savepoint");
        Assertions.assertEquals(
                CheckpointType.COMPLETED_POINT_TYPE,
                readLatestCheckpoint(checkpointStorage, jobId, 1).getCheckpointType(),
                "the savepoint fan-out persists the completion marker of the skipped pipeline");

        // Pipeline 2 completes the savepoint itself, which stores its savepoint state.
        checkpointManager
                .getCheckpointCoordinator(2)
                .completePendingCheckpoint(
                        completedCheckpoint(jobId, 2, 9L, CheckpointType.SAVEPOINT_TYPE));

        CheckpointManager restoreManager =
                createCheckpointManager(
                        jobId,
                        true,
                        RestoreMode.SAVEPOINT,
                        jobId,
                        false,
                        checkpointStorage,
                        new CheckpointConfig(),
                        2);

        Assertions.assertTrue(
                restoreManager.isCompletedBeforeRestore(1),
                "a pipeline that finished cleanly before the savepoint must stay finished on restore");
        Assertions.assertFalse(
                restoreManager.isCompletedBeforeRestore(2),
                "a pipeline with savepoint state must be restored and run again");
        Assertions.assertEquals(
                10L,
                restoreManager.getCheckpointCoordinator(2).getCheckpointIdCounter().get(),
                "the restored pipeline must continue after its stored savepoint");
    }

    /**
     * A restore may only accept a pipeline without savepoint state when the savepoint carries the
     * durable completion marker for it. A periodic checkpoint of a pipeline that never completed
     * cleanly does not explain the missing savepoint state, so the restore fails closed instead of
     * running the pipeline again and replaying the data the source job already delivered.
     */
    @Test
    public void testSavepointRestoreFailsClosedWithoutCompletionProof() throws Exception {
        long jobId = (long) (Math.random() * 1000000L);
        CheckpointStorage checkpointStorage = createCheckpointStorage();
        storeCompletedCheckpoint(checkpointStorage, jobId, 1, 5L, CheckpointType.CHECKPOINT_TYPE);

        IllegalStateException failure =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                createCheckpointManager(
                                        jobId,
                                        true,
                                        RestoreMode.SAVEPOINT,
                                        jobId,
                                        false,
                                        checkpointStorage,
                                        new CheckpointConfig(),
                                        1));
        Assertions.assertTrue(
                failure.getMessage().contains("completed-before-savepoint marker"),
                "the failure must explain that the missing savepoint state is not accounted for: "
                        + failure.getMessage());
    }

    private CompletedCheckpoint completedCheckpoint(
            long jobId, int pipelineId, long checkpointId, CheckpointType checkpointType) {
        return new CompletedCheckpoint(
                jobId,
                pipelineId,
                checkpointId,
                Instant.now().toEpochMilli(),
                checkpointType,
                Instant.now().toEpochMilli(),
                new HashMap<>(),
                new HashMap<>());
    }

    private CompletedCheckpoint readLatestCheckpoint(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId) throws Exception {
        PipelineState pipelineState =
                checkpointStorage.getLatestCheckpointByJobIdAndPipelineId(
                        String.valueOf(jobId), String.valueOf(pipelineId));
        if (pipelineState == null) {
            return null;
        }
        return new ProtoStuffSerializer()
                .deserialize(pipelineState.getStates(), CompletedCheckpoint.class);
    }
}
