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
 *       stored, and each retention call deletes exactly one {@code max-retained} block;
 *   <li>retention never removes the newest checkpoints: each pipeline always keeps at least {@code
 *       max-retained} and at most one in-flight rotation block of readable checkpoint files, all of
 *       them contiguous ids;
 *   <li>deleting one pipeline's old checkpoints never touches the sibling pipeline's files, which
 *       remain listed and readable by {@code pipelineId};
 *   <li>cancelling the job still deletes the whole job's checkpoint data across both pipelines.
 * </ul>
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
     * A pipeline must complete this many checkpoints before the first full retention rotation has
     * happened: the coordinator deletes {@code max-retained} old checkpoints once the queue reaches
     * {@code 2 * max-retained}.
     */
    private static final long COMPLETIONS_BEFORE_ROTATION = MAX_RETAINED * 2L;

    private static final int PIPELINE_COUNT = 2;

    private static final long STATE_TIMEOUT_SECONDS = 120;

    @Test
    public void testRetentionIsScopedToTheCheckpointingPipeline() throws Exception {
        String clusterName = "MultiPipelineCheckpointRetentionIT_" + System.currentTimeMillis();
        Path namespace = Files.createTempDirectory("zeta-multi-pipeline-retention-");
        RetentionTrackingStorage.reset();

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

            // ---- Phase 1: both pipelines must reach a full retention rotation.
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
                                    PipelineCheckpointOverview pipeline = entry.getValue();
                                    Assertions.assertTrue(
                                            pipeline.getCounts().getCompleted()
                                                    >= COMPLETIONS_BEFORE_ROTATION,
                                            "pipeline "
                                                    + entry.getKey()
                                                    + " must complete at least "
                                                    + COMPLETIONS_BEFORE_ROTATION
                                                    + " checkpoints before retention rotates: "
                                                    + describe(pipelines));
                                    Assertions.assertEquals(
                                            0L,
                                            pipeline.getCounts().getFailed(),
                                            "no checkpoint may fail while retention is observed: "
                                                    + describe(pipelines));
                                    Assertions.assertEquals(
                                            0L,
                                            pipeline.getCounts().getRestored(),
                                            "no pipeline may restore in this fixture: "
                                                    + describe(pipelines));
                                }
                            });
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    coordinatorService.getJobStatus(jobId),
                    "checkpoint retention must not affect the running job");

            // ---- Phase 2: per-pipeline identity, retention scope and readable recovery points.
            List<Integer> pipelineIds =
                    new ArrayList<>(pipelines(checkpointMonitor, jobId).keySet());
            Collections.sort(pipelineIds);
            Assertions.assertEquals(PIPELINE_COUNT, pipelineIds.size());

            for (Integer pipelineId : pipelineIds) {
                String pipelineIdString = String.valueOf(pipelineId);

                List<Long> storedIds =
                        new ArrayList<>(
                                RetentionTrackingStorage.storedIds(jobIdString, pipelineId));
                Collections.sort(storedIds);
                Assertions.assertTrue(
                        storedIds.size() >= MAX_RETAINED * 2L,
                        "pipeline "
                                + pipelineId
                                + " must have stored at least "
                                + MAX_RETAINED * 2L
                                + " checkpoints to observe a rotation, stored="
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

                List<RetentionTrackingStorage.DeleteRequest> deleteRequests =
                        RetentionTrackingStorage.deleteRequestsOf(pipelineId);
                Assertions.assertFalse(
                        deleteRequests.isEmpty(),
                        "pipeline "
                                + pipelineId
                                + " must have rotated its checkpoint storage after "
                                + COMPLETIONS_BEFORE_ROTATION
                                + " completions");
                Set<Long> everStored = new LinkedHashSet<>(storedIds);
                for (RetentionTrackingStorage.DeleteRequest request : deleteRequests) {
                    Assertions.assertEquals(
                            MAX_RETAINED,
                            request.getCheckpointIds().size(),
                            "retention must delete exactly one configured block at a time: "
                                    + request);
                    for (Long deletedId : request.getCheckpointIds()) {
                        Assertions.assertTrue(
                                everStored.contains(deletedId),
                                "pipeline "
                                        + pipelineId
                                        + " asked storage to delete checkpoint "
                                        + deletedId
                                        + " that it never stored successfully; stored="
                                        + storedIds);
                    }
                }

                List<Long> persistedIds =
                        persistedCheckpointIds(checkpointStorage, jobId, pipelineId);
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
                Assertions.assertTrue(
                        persistedIds.size() >= MAX_RETAINED,
                        "retention must keep the configured budget of readable checkpoints for pipeline "
                                + pipelineId
                                + ", persisted="
                                + persistedIds);
                Assertions.assertTrue(
                        persistedIds.size() <= MAX_RETAINED * 2,
                        "retention let pipeline "
                                + pipelineId
                                + " accumulate more than one in-flight block beyond the budget, "
                                + "persisted="
                                + persistedIds);
                for (int index = 1; index < persistedIds.size(); index++) {
                    Assertions.assertEquals(
                            persistedIds.get(index - 1) + 1L,
                            persistedIds.get(index).longValue(),
                            "retention must only remove the oldest checkpoints, leaving a "
                                    + "contiguous id range for pipeline "
                                    + pipelineId
                                    + ": "
                                    + persistedIds);
                }

                // The newest completed checkpoint is the pipeline's own recovery point and must
                // still be readable after older ones were rotated away.
                PipelineCheckpointOverview pipeline =
                        pipelines(checkpointMonitor, jobId).get(pipelineId);
                Assertions.assertNotNull(pipeline, "pipeline " + pipelineId + " disappeared");
                Assertions.assertNotNull(
                        pipeline.getLatestCompleted(),
                        "pipeline " + pipelineId + " reports no latest completed checkpoint");
                long latestCompletedId = pipeline.getLatestCompleted().getCheckpointId();
                Assertions.assertTrue(
                        latestCompletedId >= persistedIds.get(persistedIds.size() - 1),
                        "pipeline "
                                + pipelineId
                                + " reports latest completed checkpoint "
                                + latestCompletedId
                                + " but only retained up to "
                                + persistedIds.get(persistedIds.size() - 1));
                Assertions.assertTrue(
                        everStored.contains(latestCompletedId),
                        "the latest completed checkpoint of pipeline "
                                + pipelineId
                                + " was never stored: "
                                + latestCompletedId);

                PipelineState latest = latestStoredCheckpoint(checkpointStorage, jobId, pipelineId);
                Assertions.assertNotNull(
                        latest, "storage exposes no latest checkpoint for pipeline " + pipelineId);
                Assertions.assertFalse(
                        latest.getStates() == null || latest.getStates().length == 0,
                        "the retained latest checkpoint of pipeline "
                                + pipelineId
                                + " has no state payload");
            }

            // ---- Phase 3: deletion of one pipeline's checkpoints never removed a sibling's
            // newest recovery point, which is the property a job-scoped delete path would break.
            Set<String> latestIdsByPipeline = new LinkedHashSet<>();
            for (Integer pipelineId : pipelineIds) {
                PipelineState latest = latestStoredCheckpoint(checkpointStorage, jobId, pipelineId);
                Assertions.assertNotNull(latest);
                Assertions.assertEquals(
                        pipelineId.intValue(),
                        latest.getPipelineId(),
                        "the latest checkpoint must be reported for the pipeline the client asked "
                                + "for");
                latestIdsByPipeline.add(pipelineId + ":" + latest.getCheckpointId());
            }
            Assertions.assertEquals(
                    PIPELINE_COUNT,
                    latestIdsByPipeline.size(),
                    "each pipeline must keep its own latest checkpoint: " + latestIdsByPipeline);

            // ---- Phase 4: cancellation still cleans checkpoint data for the whole job.
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
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(
                            file -> {
                                try {
                                    Files.deleteIfExists(file);
                                } catch (Exception e) {
                                    log.info("Failed to delete {}", file, e);
                                }
                            });
        } catch (Exception e) {
            log.info("Failed to clean checkpoint namespace {}", path, e);
        }
    }
}
