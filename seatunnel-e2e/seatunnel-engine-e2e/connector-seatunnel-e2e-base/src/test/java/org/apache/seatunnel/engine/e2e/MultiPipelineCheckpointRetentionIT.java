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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.e2e.retention.RetentionTrackingStorage;
import org.apache.seatunnel.engine.e2e.retention.RetentionTrackingStorageFactory;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Regression guard for checkpoint retention and checkpoint identity on a multi-pipeline job.
 *
 * <p>One Zeta job can own several independent pipelines (here two {@code FakeSource -> Console}
 * chains), and every pipeline runs its own checkpoint coordinator with its own checkpoint id
 * sequence and its own retention budget. Those are per-pipeline contracts, not per-job ones: the
 * checkpoint id counter is keyed by {@code jobId + pipelineId}, checkpoint files live under a
 * per-pipeline storage path, and automatic retention deletes blocks of that pipeline's own oldest
 * checkpoints when the configured {@code max-retained} budget is exceeded.
 *
 * <p>This test pins the observable consequences of that contract while both pipelines checkpoint
 * concurrently:
 *
 * <ul>
 *   <li>each pipeline starts its checkpoint id sequence at 1 and keeps it gap-free, so one
 *       pipeline's checkpointing can never consume or skip another pipeline's ids;
 *   <li>every id a pipeline asks storage to delete is an id that the same pipeline had already
 *       stored, each retention call deletes exactly one {@code max-retained} block, and the delete
 *       requests cover exactly the oldest ids that are no longer part of the retained window;
 *   <li>retention never removes the newest checkpoints: each pipeline always keeps at least {@code
 *       max-retained} and at most one in-flight rotation block of readable checkpoint files, all of
 *       them contiguous ids ending at the pipeline's newest completed checkpoint, which is also the
 *       recovery point the storage and the monitor return;
 *   <li>deleting one pipeline's old checkpoints never touches the sibling pipeline's files, which
 *       remain listed and readable by {@code pipelineId};
 *   <li>cancelling the job still deletes the whole job's checkpoint data across both pipelines.
 * </ul>
 *
 * <p>The per-pipeline assertions run at a frozen observation boundary instead of against storage
 * that the 500 ms scheduler keeps mutating. The storage fixture parks a coordinator before it
 * stores the next block ({@link RetentionTrackingStorage#armAsymmetricFence}), which makes the
 * recorded ids, the delete requests, the files on disk and the checkpoint monitor a single coherent
 * view. One pipeline is parked before its first rotation and the other after it, so the deleting
 * pipeline asks storage to remove checkpoint ids that are exactly the sibling's only recovery
 * points: a job-scoped delete path would remove the sibling's files and fail the test.
 *
 * <p>Existing coverage exercises single-pipeline retention ({@code
 * CheckpointStorageRetentionPruneTest}) and cross-job slot isolation, but no E2E observes retention
 * while two pipelines of the same job checkpoint in parallel, which is exactly where a job-scoped
 * counter or a job-scoped delete path would corrupt a sibling pipeline's recovery points.
 */
@Slf4j
public class MultiPipelineCheckpointRetentionIT {

    private static final String CONF_FILE =
            "multi-pipeline-retention/stream_two_pipelines_to_console_checkpoint_retention.conf";

    /** Retention budget configured for every pipeline of the job. */
    private static final int MAX_RETAINED = 2;

    /**
     * The anchor pipeline is parked at this completed checkpoint count, before its first retention
     * rotation has deleted anything, so all of its checkpoint ids remain its recovery points.
     */
    private static final long ANCHOR_COMPLETIONS = MAX_RETAINED;

    /**
     * The rotating pipeline is parked at this completed checkpoint count, after its first full
     * retention rotation: the coordinator deletes {@code max-retained} old checkpoints once the
     * queue reaches {@code 2 * max-retained}. Its deletions therefore overlap the anchor's ids.
     */
    private static final long ROTATOR_COMPLETIONS = MAX_RETAINED * 2L;

    private static final int PIPELINE_COUNT = 2;

    private static final long STATE_TIMEOUT_SECONDS = 120;

    @Test
    public void testRetentionIsScopedToTheCheckpointingPipeline() throws Exception {
        String clusterName = "MultiPipelineCheckpointRetentionIT_" + System.currentTimeMillis();
        Path namespace = Files.createTempDirectory("zeta-multi-pipeline-retention-");
        RetentionTrackingStorage.reset();
        // Fence checkpoint progress before the job starts: the first pipeline to reach the anchor
        // budget stops before its first rotation, the other stops after it rotated the same ids.
        RetentionTrackingStorage.armAsymmetricFence(ANCHOR_COMPLETIONS, ROTATOR_COMPLETIONS);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;
        try {
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // Fixed slots large enough for both chained pipelines plus their enumerators, so the
            // job runs immediately and the test observes checkpointing rather than admission.
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(6);

            CheckpointStorageConfig storageConfig =
                    seaTunnelConfig.getEngineConfig().getCheckpointConfig().getStorage();
            storageConfig.setStorage(RetentionTrackingStorageFactory.IDENTIFIER);
            storageConfig.getStoragePluginConfig().put("namespace", namespace.toString());
            storageConfig.setMaxRetainedCheckpoints(MAX_RETAINED);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            SeaTunnelServer server = node.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService coordinatorService = server.getCoordinatorService();
            CheckpointMonitorService checkpointMonitor = server.getCheckpointMonitorService();
            CheckpointStorage checkpointStorage =
                    server.getCheckpointService().getCheckpointStorage();
            Assertions.assertTrue(
                    checkpointStorage instanceof RetentionTrackingStorage,
                    "the test storage plugin must be active, but got "
                            + checkpointStorage.getClass().getName());

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("multi-pipeline-checkpoint-retention");
            ClientJobExecutionEnvironment executionEnvironment =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(CONF_FILE), jobConfig, seaTunnelConfig);
            job = executionEnvironment.execute();
            long jobId = job.getJobId();
            String jobIdString = String.valueOf(jobId);

            awaitJobStatus(coordinatorService, jobId, JobStatus.RUNNING);

            // ---- Phase 1: park both pipelines on a common observation boundary. The anchor stops
            // before its first rotation, the rotating pipeline stops after it rotated the very ids
            // the anchor still holds, so nothing the assertions below read is moving any more.
            Assertions.assertTrue(
                    RetentionTrackingStorage.awaitFenceHeld(
                            PIPELINE_COUNT, STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "both pipelines must reach the retention fence: "
                            + describe(pipelines(checkpointMonitor, jobId)));
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    coordinatorService.getJobStatus(jobId),
                    "checkpoint retention must not affect the running job");

            Map<Integer, PipelineCheckpointOverview> frozen = pipelines(checkpointMonitor, jobId);
            Assertions.assertEquals(
                    PIPELINE_COUNT,
                    frozen.size(),
                    "the job must report both pipelines: " + describe(frozen));

            Map<Integer, Integer> storedCounts = new HashMap<>();
            for (Integer pipelineId : frozen.keySet()) {
                storedCounts.put(
                        pipelineId,
                        RetentionTrackingStorage.storedIds(jobIdString, pipelineId).size());
            }
            Integer anchorPipelineId = pipelineAtStoredCount(storedCounts, ANCHOR_COMPLETIONS);
            Integer rotatingPipelineId = pipelineAtStoredCount(storedCounts, ROTATOR_COMPLETIONS);
            Assertions.assertNotEquals(
                    anchorPipelineId,
                    rotatingPipelineId,
                    "the fence must park one pipeline before its first rotation and the other after "
                            + "it, but stored counts were "
                            + storedCounts);

            // ---- Phase 2: exact per-pipeline retention ledger at the frozen boundary. The anchor
            // has not rotated anything yet; the rotating pipeline has stored one block more than
            // the budget and must have deleted its own oldest block, never the sibling's.
            List<Long> anchoredIds =
                    assertRetentionLedger(
                            checkpointStorage,
                            jobId,
                            anchorPipelineId,
                            frozen.get(anchorPipelineId));
            Assertions.assertEquals(
                    ANCHOR_COMPLETIONS,
                    frozen.get(anchorPipelineId).getCounts().getCompleted(),
                    "the anchor pipeline must be parked exactly at its recovery point: "
                            + describe(frozen));

            List<Long> rotatedIds =
                    assertRetentionLedger(
                            checkpointStorage,
                            jobId,
                            rotatingPipelineId,
                            frozen.get(rotatingPipelineId));
            Assertions.assertEquals(
                    ROTATOR_COMPLETIONS,
                    frozen.get(rotatingPipelineId).getCounts().getCompleted(),
                    "the rotating pipeline must be parked right after its first rotation: "
                            + describe(frozen));

            Assertions.assertTrue(
                    latestCompletedId(frozen.get(anchorPipelineId))
                            != latestCompletedId(frozen.get(rotatingPipelineId)),
                    "the two pipelines must publish independent recovery points: "
                            + describe(frozen));

            // ---- Phase 3: the rotating pipeline asked storage to delete checkpoint ids that are
            // exactly the anchor's only recovery points, and the anchor's files survived; a
            // job-scoped delete path would have removed them.
            List<Long> deletedByRotating = deletedCheckpointIds(rotatingPipelineId);
            Assertions.assertTrue(
                    deletedByRotating.containsAll(anchoredIds),
                    "the rotating pipeline must delete checkpoint ids overlapping the anchor's "
                            + "recovery points, deleted="
                            + deletedByRotating
                            + ", anchored="
                            + anchoredIds);
            Assertions.assertTrue(
                    RetentionTrackingStorage.deleteRequestsOf(anchorPipelineId).isEmpty(),
                    "the anchor pipeline must not have rotated yet, delete requests="
                            + RetentionTrackingStorage.deleteRequestsOf(anchorPipelineId));
            Assertions.assertEquals(
                    0,
                    RetentionTrackingStorage.jobLevelDeletes(),
                    "rotating the oldest block of one pipeline must not delete the whole job's "
                            + "checkpoint data");
            Assertions.assertEquals(
                    anchoredIds,
                    persistedCheckpointIds(checkpointStorage, jobId, anchorPipelineId),
                    "a rotation of the other pipeline must not remove the anchor's checkpoint "
                            + "files, expected="
                            + anchoredIds);
            for (Long anchoredId : anchoredIds) {
                PipelineState anchored =
                        storedCheckpoint(checkpointStorage, jobId, anchorPipelineId, anchoredId);
                Assertions.assertNotNull(
                        anchored,
                        "the anchor's checkpoint "
                                + anchoredId
                                + " must stay readable after the other pipeline rotated");
                Assertions.assertEquals(
                        anchoredId.longValue(),
                        anchored.getCheckpointId(),
                        "storage must return the anchor checkpoint that was asked for");
                Assertions.assertTrue(
                        anchored.getStates() != null && anchored.getStates().length > 0,
                        "the anchor's checkpoint " + anchoredId + " has no state payload");
            }

            // ---- Phase 4: let the anchor rotate too, then take a second symmetric boundary so
            // both pipelines are asserted again after each of them has rotated at least once.
            RetentionTrackingStorage.releaseFence();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Map<Integer, PipelineCheckpointOverview> pipelines =
                                        pipelines(checkpointMonitor, jobId);
                                Assertions.assertEquals(
                                        PIPELINE_COUNT,
                                        pipelines.size(),
                                        "the job must report both pipelines: "
                                                + describe(pipelines));
                                for (Map.Entry<Integer, PipelineCheckpointOverview> entry :
                                        pipelines.entrySet()) {
                                    Assertions.assertTrue(
                                            entry.getValue().getCounts().getCompleted()
                                                    >= ROTATOR_COMPLETIONS,
                                            "pipeline "
                                                    + entry.getKey()
                                                    + " must complete at least "
                                                    + ROTATOR_COMPLETIONS
                                                    + " checkpoints after the fence is released: "
                                                    + describe(pipelines));
                                    Assertions.assertFalse(
                                            RetentionTrackingStorage.deleteRequestsOf(
                                                            entry.getKey())
                                                    .isEmpty(),
                                            "pipeline "
                                                    + entry.getKey()
                                                    + " must rotate its own checkpoint storage: "
                                                    + describe(pipelines));
                                    Assertions.assertEquals(
                                            0L,
                                            entry.getValue().getCounts().getFailed(),
                                            "no checkpoint may fail while retention is observed: "
                                                    + describe(pipelines));
                                    Assertions.assertEquals(
                                            0L,
                                            entry.getValue().getCounts().getRestored(),
                                            "no pipeline may restore in this fixture: "
                                                    + describe(pipelines));
                                }
                            });

            RetentionTrackingStorage.armFenceForPipelines(frozen.keySet());
            Assertions.assertTrue(
                    RetentionTrackingStorage.awaitFenceHeld(
                            PIPELINE_COUNT, STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "both pipelines must reach the second retention fence: "
                            + describe(pipelines(checkpointMonitor, jobId)));
            Map<Integer, PipelineCheckpointOverview> symmetric =
                    pipelines(checkpointMonitor, jobId);
            Assertions.assertEquals(
                    PIPELINE_COUNT,
                    symmetric.size(),
                    "the job must report both pipelines: " + describe(symmetric));
            for (Map.Entry<Integer, PipelineCheckpointOverview> entry : symmetric.entrySet()) {
                assertRetentionLedger(checkpointStorage, jobId, entry.getKey(), entry.getValue());
            }
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    coordinatorService.getJobStatus(jobId),
                    "checkpoint retention must not affect the running job");
            RetentionTrackingStorage.releaseFence();

            // ---- Phase 5: cancellation still cleans checkpoint data for the whole job.
            job.cancelJob();
            awaitJobStatus(coordinatorService, jobId, JobStatus.CANCELED);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            getAllCheckpoints(checkpointStorage, jobId).isEmpty(),
                                            "cancelling the job must delete the checkpoint data of "
                                                    + "all its pipelines"));
            Assertions.assertTrue(
                    RetentionTrackingStorage.jobLevelDeletes() >= 1,
                    "the job-level checkpoint cleanup must have been requested");
        } finally {
            RetentionTrackingStorage.releaseFence();
            if (job != null) {
                try {
                    job.cancelJob();
                } catch (Throwable e) {
                    log.info("Ignoring failure while cancelling job during cleanup", e);
                }
            }
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
            deleteRecursively(namespace);
        }
    }

    /**
     * Asserts the entire per-pipeline retention ledger from the frozen fixture and returns the
     * checkpoint ids that are still retained and readable.
     *
     * <p>The checker does not only verify contiguity: it pins the recovery point at the newest end
     * of the retained window. The newest completed checkpoint must still be stored, listed and
     * readable, the storage latest-checkpoint lookup must return exactly that id, and the retention
     * deletes must be exactly the oldest ids that are no longer part of the retained window.
     * Keeping only an older window while deleting the newest checkpoints therefore fails.
     */
    private static List<Long> assertRetentionLedger(
            CheckpointStorage checkpointStorage,
            long jobId,
            int pipelineId,
            PipelineCheckpointOverview pipeline) {
        long completed = pipeline.getCounts().getCompleted();
        Assertions.assertEquals(
                0L,
                pipeline.getCounts().getFailed(),
                "no checkpoint may fail while retention is observed: " + pipeline.getCounts());
        Assertions.assertEquals(
                0L,
                pipeline.getCounts().getRestored(),
                "no pipeline may restore in this fixture: " + pipeline.getCounts());
        Assertions.assertTrue(
                completed >= MAX_RETAINED,
                "pipeline "
                        + pipelineId
                        + " must complete at least "
                        + MAX_RETAINED
                        + " checkpoints, completed="
                        + completed);

        List<Long> storedIds =
                new ArrayList<>(
                        RetentionTrackingStorage.storedIds(String.valueOf(jobId), pipelineId));
        Collections.sort(storedIds);
        Assertions.assertEquals(
                completed,
                storedIds.size(),
                "pipeline "
                        + pipelineId
                        + " must store exactly one checkpoint per completion, completed="
                        + completed
                        + ", stored="
                        + storedIds);
        Assertions.assertEquals(
                new LinkedHashSet<>(storedIds).size(),
                storedIds.size(),
                "a pipeline must never store the same checkpoint id twice: " + storedIds);
        for (int index = 0; index < storedIds.size(); index++) {
            Assertions.assertEquals(
                    index + 1L,
                    storedIds.get(index).longValue(),
                    "pipeline "
                            + pipelineId
                            + " must own a gap-free checkpoint id sequence starting at 1; "
                            + "another pipeline's checkpointing must never consume its ids: "
                            + storedIds);
        }
        Set<Long> everStored = new LinkedHashSet<>(storedIds);

        List<RetentionTrackingStorage.DeleteRequest> deleteRequests =
                RetentionTrackingStorage.deleteRequestsOf(pipelineId);
        long rotations = Math.max(0L, completed / MAX_RETAINED - 1L);
        Assertions.assertEquals(
                rotations,
                deleteRequests.size(),
                "pipeline "
                        + pipelineId
                        + " must rotate exactly once per full block beyond the budget "
                        + MAX_RETAINED
                        + ": "
                        + deleteRequests);
        List<Long> deletedIds = new ArrayList<>();
        for (RetentionTrackingStorage.DeleteRequest request : deleteRequests) {
            Assertions.assertEquals(
                    MAX_RETAINED,
                    request.getCheckpointIds().size(),
                    "retention must delete exactly one configured block at a time: " + request);
            for (Long deletedId : request.getCheckpointIds()) {
                Assertions.assertTrue(
                        everStored.contains(deletedId),
                        "pipeline "
                                + pipelineId
                                + " asked storage to delete checkpoint "
                                + deletedId
                                + " that it never stored successfully; stored="
                                + storedIds);
                deletedIds.add(deletedId);
            }
        }
        Collections.sort(deletedIds);
        Assertions.assertEquals(
                new LinkedHashSet<>(deletedIds).size(),
                deletedIds.size(),
                "retention must not delete the same checkpoint id twice: " + deletedIds);

        List<Long> persistedIds = persistedCheckpointIds(checkpointStorage, jobId, pipelineId);
        Assertions.assertFalse(
                persistedIds.isEmpty(),
                "pipeline " + pipelineId + " retains no readable checkpoint file");
        Assertions.assertEquals(
                new LinkedHashSet<>(persistedIds).size(),
                persistedIds.size(),
                "a checkpoint file must be listed exactly once: " + persistedIds);
        for (Long persistedId : persistedIds) {
            Assertions.assertTrue(
                    everStored.contains(persistedId),
                    "pipeline "
                            + pipelineId
                            + " lists checkpoint "
                            + persistedId
                            + " that it never stored; stored="
                            + storedIds);
        }

        int retainedCount = persistedIds.size();
        Assertions.assertTrue(
                retainedCount >= MAX_RETAINED,
                "retention must keep the configured budget of readable checkpoints for pipeline "
                        + pipelineId
                        + ", persisted="
                        + persistedIds);
        Assertions.assertTrue(
                retainedCount <= MAX_RETAINED * 2,
                "retention let pipeline "
                        + pipelineId
                        + " accumulate more than one in-flight block beyond the budget, "
                        + "persisted="
                        + persistedIds);

        CheckpointInfo latestCompleted = pipeline.getLatestCompleted();
        Assertions.assertNotNull(
                latestCompleted,
                "pipeline " + pipelineId + " reports no latest completed checkpoint");
        long latestCompletedId = latestCompleted.getCheckpointId();
        Assertions.assertEquals(
                completed,
                latestCompletedId,
                "the monitor must report the last completed checkpoint id of pipeline "
                        + pipelineId
                        + ", completed="
                        + completed);

        // The newest completed checkpoint is the pipeline's own recovery point: it must be the
        // newest retained file, not merely some id below the maximum.
        Assertions.assertEquals(
                latestCompletedId,
                persistedIds.get(retainedCount - 1).longValue(),
                "the newest completed checkpoint of pipeline "
                        + pipelineId
                        + " must still be retained, persisted="
                        + persistedIds
                        + ", latestCompleted="
                        + latestCompletedId);
        List<Long> expectedPersisted =
                ascending(latestCompletedId - retainedCount + 1L, latestCompletedId);
        Assertions.assertEquals(
                expectedPersisted,
                persistedIds,
                "retention must keep the newest contiguous window and only drop the oldest "
                        + "checkpoints of pipeline "
                        + pipelineId);
        Assertions.assertEquals(
                ascending(1L, latestCompletedId - retainedCount),
                deletedIds,
                "retention must delete exactly the oldest checkpoint ids that fall out of the "
                        + "window of pipeline "
                        + pipelineId);

        PipelineState latest = latestStoredCheckpoint(checkpointStorage, jobId, pipelineId);
        Assertions.assertNotNull(
                latest, "storage exposes no latest checkpoint for pipeline " + pipelineId);
        Assertions.assertEquals(
                latestCompletedId,
                latest.getCheckpointId(),
                "storage must return the newest completed checkpoint of pipeline " + pipelineId);
        Assertions.assertEquals(
                pipelineId,
                latest.getPipelineId(),
                "the latest checkpoint must be reported for the pipeline the client asked for");
        Assertions.assertTrue(
                latest.getStates() != null && latest.getStates().length > 0,
                "the retained latest checkpoint of pipeline "
                        + pipelineId
                        + " has no state payload");

        for (Long retainedId : persistedIds) {
            PipelineState retained =
                    storedCheckpoint(checkpointStorage, jobId, pipelineId, retainedId);
            Assertions.assertNotNull(
                    retained,
                    "retained checkpoint "
                            + retainedId
                            + " of pipeline "
                            + pipelineId
                            + " must be readable");
            Assertions.assertEquals(
                    retainedId.longValue(),
                    retained.getCheckpointId(),
                    "storage must return the retained checkpoint that was asked for");
            Assertions.assertTrue(
                    retained.getStates() != null && retained.getStates().length > 0,
                    "the retained checkpoint "
                            + retainedId
                            + " of pipeline "
                            + pipelineId
                            + " has no state payload");
        }
        return persistedIds;
    }

    private static Integer pipelineAtStoredCount(
            Map<Integer, Integer> storedCounts, long expectedStoredCount) {
        List<Integer> matches = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : storedCounts.entrySet()) {
            if (entry.getValue() == expectedStoredCount) {
                matches.add(entry.getKey());
            }
        }
        Assertions.assertEquals(
                1,
                matches.size(),
                "exactly one pipeline must be parked with "
                        + expectedStoredCount
                        + " stored checkpoints, but stored counts were "
                        + storedCounts);
        return matches.get(0);
    }

    private static long latestCompletedId(PipelineCheckpointOverview pipeline) {
        return pipeline.getLatestCompleted().getCheckpointId();
    }

    private static List<Long> deletedCheckpointIds(int pipelineId) {
        List<Long> deletedIds = new ArrayList<>();
        for (RetentionTrackingStorage.DeleteRequest request :
                RetentionTrackingStorage.deleteRequestsOf(pipelineId)) {
            deletedIds.addAll(request.getCheckpointIds());
        }
        Collections.sort(deletedIds);
        return deletedIds;
    }

    private static List<Long> ascending(long fromInclusive, long toInclusive) {
        List<Long> ids = new ArrayList<>();
        for (long id = fromInclusive; id <= toInclusive; id++) {
            ids.add(id);
        }
        return ids;
    }

    private static Map<Integer, PipelineCheckpointOverview> pipelines(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(CheckpointOverview::getPipelines)
                .map(HashMap::new)
                .orElseGet(HashMap::new);
    }

    private static List<Long> persistedCheckpointIds(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId) {
        List<PipelineState> states = checkpointsOfPipeline(checkpointStorage, jobId, pipelineId);
        return states.stream()
                .map(PipelineState::getCheckpointId)
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
    }

    private static PipelineState latestStoredCheckpoint(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId) {
        try {
            return checkpointStorage.getLatestCheckpointByJobIdAndPipelineId(
                    String.valueOf(jobId), String.valueOf(pipelineId));
        } catch (CheckpointStorageException e) {
            return Assertions.fail(
                    "Failed to read the latest checkpoint of job "
                            + jobId
                            + ", pipeline "
                            + pipelineId,
                    e);
        }
    }

    private static PipelineState storedCheckpoint(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId, long checkpointId) {
        try {
            return checkpointStorage.getCheckpoint(
                    String.valueOf(jobId),
                    String.valueOf(pipelineId),
                    String.valueOf(checkpointId));
        } catch (CheckpointStorageException e) {
            return Assertions.fail(
                    "Failed to read checkpoint "
                            + checkpointId
                            + " of job "
                            + jobId
                            + ", pipeline "
                            + pipelineId,
                    e);
        }
    }

    private static List<PipelineState> checkpointsOfPipeline(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId) {
        try {
            return checkpointStorage.getCheckpointsByJobIdAndPipelineId(
                    String.valueOf(jobId), String.valueOf(pipelineId));
        } catch (CheckpointStorageException e) {
            return Assertions.fail(
                    "Failed to list checkpoints of job " + jobId + ", pipeline " + pipelineId, e);
        }
    }

    private static List<PipelineState> getAllCheckpoints(
            CheckpointStorage checkpointStorage, long jobId) {
        try {
            return checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        } catch (CheckpointStorageException e) {
            return Assertions.fail("Failed to list checkpoints of job " + jobId, e);
        }
    }

    private static void awaitJobStatus(
            CoordinatorService coordinatorService, long jobId, JobStatus expected) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        coordinatorService.getJobStatus(jobId),
                                        "status of job " + jobId));
    }

    private static String describe(Map<Integer, PipelineCheckpointOverview> pipelines) {
        StringBuilder builder = new StringBuilder("{");
        pipelines.forEach(
                (pipelineId, pipeline) ->
                        builder.append("pipeline ")
                                .append(pipelineId)
                                .append(" -> ")
                                .append(pipeline.getCounts())
                                .append(", "));
        return builder.append('}').toString();
    }

    private static void deleteRecursively(Path path) {
        try {
            if (path == null || !Files.exists(path)) {
                return;
            }
            try (Stream<Path> walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(
                                file -> {
                                    try {
                                        Files.deleteIfExists(file);
                                    } catch (Exception e) {
                                        log.info("Failed to delete {}", file, e);
                                    }
                                });
            }
        } catch (Exception e) {
            log.info("Failed to clean checkpoint namespace {}", path, e);
        }
    }
}
