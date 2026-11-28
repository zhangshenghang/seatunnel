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

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.e2e.singleflight.CheckpointSingleFlightSink;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Guards the Zeta checkpoint scheduler's single-flight contract when a checkpoint outlives several
 * trigger intervals.
 *
 * <p>The job runs with {@code checkpoint.interval = 500ms} and a test-only sink that parks inside
 * the first {@link org.apache.seatunnel.api.sink.SinkWriter#snapshotState(long)} call until the
 * test releases it (the checkpoint timeout is 30s, so the barrier is genuinely pending, not
 * expiring). While that checkpoint is provably in flight, the master must:
 *
 * <ul>
 *   <li>not create a second pending checkpoint, even though the periodic trigger fires several more
 *       times: the trigger has to be skipped and re-armed, which is observable both in the
 *       checkpoint overview ({@code triggered == 1}, {@code inProgress == 1} for the held id) and
 *       at the sink (exactly one snapshot request);
 *   <li>not publish the held checkpoint as completed or failed before the task acknowledges it, and
 *       not treat a skipped trigger as a checkpoint failure (with {@code job.retry.times = 0} a
 *       spurious failure would terminate the job instead of restoring it);
 *   <li>keep the job {@code RUNNING} with no pipeline restore.
 * </ul>
 *
 * <p>After the release, the held checkpoint must complete exactly once and the trigger chain must
 * still be alive: a later checkpoint with a strictly greater id must complete, ids must never be
 * reused, and the job must stay {@code RUNNING}. Finally, cancelling the job must reach {@code
 * CANCELED}, release the pipeline's fixed slots and leave the sink without further snapshot
 * requests once the terminal state has settled.
 *
 * <p>Why this boundary has real regression risk: the single-flight decision is one {@code
 * pendingCounter > 0} branch in {@code CheckpointCoordinator#tryTriggerPendingCheckpoint}. If it
 * regresses, two barriers are injected into the same pipeline and two snapshots run concurrently,
 * doubling snapshot load and allowing a checkpoint to be completed from a mix of states. If the
 * skipped trigger is not re-armed, checkpointing stops forever with no task error and no failed
 * job, so only the checkpoint overview and the sink-side snapshot record can show it. Existing
 * coverage does not reach it: {@code CheckpointEnableIT} only checks savepoint/restore, {@code
 * CheckpointCoordinatorFailoverIT} switches the master between checkpoints, and every other
 * streaming test lets each snapshot answer immediately, so no test holds one checkpoint across
 * several trigger intervals.
 */
public class CheckpointSingleFlightIT {

    private static final String JOB_CONFIG_FILE = "stream_fake_to_single_flight_hold_sink.conf";

    /** The job has exactly one FakeSource -&gt; test sink pipeline. */
    private static final int PIPELINE_ID = 1;

    /** Must match {@code checkpoint.interval} in the job config. */
    private static final long CHECKPOINT_INTERVAL_MS = 500L;

    /**
     * How long the first checkpoint is held. Several trigger periods, so the skipped-trigger path
     * is exercised repeatedly rather than once.
     */
    private static final long HOLD_MILLIS = 3_500L;

    /**
     * Settling time after the job is terminal before sampling for post-terminal snapshot requests.
     * A barrier that was already in flight when the cancel arrived may still reach the sink during
     * task draining; only requests after that drain are a scheduling bug.
     */
    private static final long TERMINAL_SETTLE_MILLIS = 1_500L;

    private static final long POLL_INTERVAL_MILLIS = 100L;
    private static final long JOB_START_TIMEOUT_SECONDS = 120L;
    private static final long CHECKPOINT_PROGRESS_TIMEOUT_SECONDS = 60L;

    /**
     * Upper bound for the cancellation wait, so a job that never reaches a terminal state fails
     * this test instead of blocking until the external CI job timeout.
     */
    private static final long TERMINAL_STATUS_TIMEOUT_SECONDS = 60L;

    @Test
    public void testSlowCheckpointStaysSingleFlightAndKeepsTheTriggerChainAlive() throws Exception {
        CheckpointSingleFlightSink.reset();

        String testCaseName = "testSlowCheckpointStaysSingleFlightAndKeepsTheTriggerChainAlive";
        String clusterName = "CheckpointSingleFlightIT_" + testCaseName;
        String checkpointNamespace =
                System.getProperty("java.io.tmpdir")
                        + File.separator
                        + "seatunnel-checkpoint-single-flight-"
                        + System.nanoTime()
                        + File.separator;

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        try {
            SeaTunnelConfig config = getSeaTunnelConfig(clusterName, checkpointNamespace);
            node = SeaTunnelServerStarter.createHazelcastInstance(config);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(clusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(JOB_CONFIG_FILE), jobConfig, config);
            ClientJobProxy jobProxy = jobExecutionEnv.execute();
            long jobId = jobProxy.getJobId();

            HazelcastInstanceImpl finalNode = node;
            SeaTunnelServer server =
                    finalNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

            Awaitility.await()
                    .atMost(JOB_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, jobProxy.getJobStatus()));

            // 1. The sink parks inside the first checkpoint's snapshot, so from here on exactly
            //    one checkpoint is in flight and it cannot complete on its own.
            Assertions.assertTrue(
                    CheckpointSingleFlightSink.awaitFirstSnapshot(
                            JOB_START_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "the sink never started the first checkpoint snapshot");
            List<Long> startedSnapshotIds = CheckpointSingleFlightSink.snapshotIds();
            Assertions.assertEquals(
                    1,
                    startedSnapshotIds.size(),
                    "the sink must be asked for exactly the first checkpoint snapshot, got "
                            + startedSnapshotIds);
            long heldCheckpointId = startedSnapshotIds.get(0);

            awaitOverview(
                    server,
                    jobId,
                    pipeline ->
                            Assertions.assertEquals(
                                    1,
                                    pipeline.getInProgress().size(),
                                    "the held checkpoint must be the only one in progress"));

            // 2. Hold the checkpoint across several trigger periods. Every periodic trigger that
            //    fires in this window must be skipped and re-armed, not turned into a second
            //    pending checkpoint and not turned into a checkpoint failure.
            long holdDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HOLD_MILLIS);
            int samples = 0;
            while (System.nanoTime() < holdDeadline) {
                PipelineCheckpointOverview pipeline = pipelineOverview(server, jobId);
                Assertions.assertNotNull(
                        pipeline,
                        "the checkpoint overview disappeared while a checkpoint was held");
                Assertions.assertEquals(
                        1L,
                        pipeline.getCounts().getTriggered(),
                        "exactly one checkpoint must have been created while the first was held, "
                                + "counts="
                                + pipeline.getCounts());
                Assertions.assertEquals(
                        1L,
                        pipeline.getCounts().getInProgress(),
                        "the held checkpoint must stay the only in-progress checkpoint, counts="
                                + pipeline.getCounts());
                Assertions.assertEquals(
                        0L,
                        pipeline.getCounts().getCompleted(),
                        "a held checkpoint must not be published as completed before its task "
                                + "acknowledges it");
                Assertions.assertEquals(
                        0L,
                        pipeline.getCounts().getFailed(),
                        "skipping a periodic trigger is not a checkpoint failure");
                Assertions.assertEquals(
                        0L,
                        pipeline.getCounts().getRestored(),
                        "a skipped periodic trigger must not restore the pipeline");
                Assertions.assertNull(
                        pipeline.getLatestFailed(),
                        "no checkpoint may be recorded as failed while the first one is held");
                Assertions.assertNull(
                        pipeline.getLatestCompleted(),
                        "no checkpoint may be recorded as completed while the first one is held");
                Assertions.assertEquals(
                        1,
                        pipeline.getInProgress().size(),
                        "only the held checkpoint may be in progress, overview=" + pipeline);
                Assertions.assertEquals(
                        heldCheckpointId,
                        pipeline.getInProgress().get(0).getCheckpointId(),
                        "the in-progress checkpoint must remain the held one");
                Assertions.assertEquals(
                        1,
                        CheckpointSingleFlightSink.snapshotCallCount(),
                        "the coordinator must not ask the sink for a second snapshot while one "
                                + "checkpoint is still in flight, snapshots="
                                + CheckpointSingleFlightSink.snapshotIds());
                Assertions.assertEquals(JobStatus.RUNNING, jobProxy.getJobStatus());
                samples++;
                Thread.sleep(POLL_INTERVAL_MILLIS);
            }
            Assertions.assertTrue(
                    samples >= (HOLD_MILLIS / CHECKPOINT_INTERVAL_MS) / 2,
                    "expected to sample the held checkpoint across the hold window, got "
                            + samples
                            + " samples");

            // 3. Release the held checkpoint: it must complete exactly once. Waiting through the
            //    overview history (instead of a single counter read) keeps this stable when the
            //    next checkpoint completes before the assertion is evaluated.
            CheckpointSingleFlightSink.releaseFirstSnapshot();
            Awaitility.await()
                    .atMost(CHECKPOINT_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview pipeline =
                                        pipelineOverview(server, jobId);
                                Assertions.assertNotNull(pipeline);
                                Assertions.assertEquals(
                                        1L,
                                        completedOccurrences(pipeline, heldCheckpointId),
                                        "the released checkpoint must be recorded as completed "
                                                + "exactly once, history="
                                                + pipeline.getHistory());
                            });

            // 4. The skipped triggers must not have broken the chain: another checkpoint with a
            //    greater id must complete, without failures and without a pipeline restore.
            Awaitility.await()
                    .atMost(CHECKPOINT_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview pipeline =
                                        pipelineOverview(server, jobId);
                                Assertions.assertNotNull(pipeline);
                                Assertions.assertTrue(
                                        hasCompletedAfter(pipeline, heldCheckpointId),
                                        "checkpointing must continue after the held checkpoint "
                                                + "completed, history="
                                                + pipeline.getHistory());
                                Assertions.assertEquals(
                                        0L,
                                        pipeline.getCounts().getFailed(),
                                        "no checkpoint may fail during the whole scenario");
                                Assertions.assertEquals(
                                        0L,
                                        pipeline.getCounts().getRestored(),
                                        "no pipeline restore may happen during the whole "
                                                + "scenario");
                                Assertions.assertEquals(JobStatus.RUNNING, jobProxy.getJobStatus());
                            });

            // 5. Every triggered checkpoint maps to exactly one snapshot request, in strictly
            //    increasing id order: the scheduler never overlaps two checkpoints.
            List<Long> finalSnapshotIds = CheckpointSingleFlightSink.snapshotIds();
            Assertions.assertTrue(
                    finalSnapshotIds.size() >= 2,
                    "expected at least two checkpoints after the release, got " + finalSnapshotIds);
            Assertions.assertEquals(
                    finalSnapshotIds.size(),
                    new HashSet<>(finalSnapshotIds).size(),
                    "a checkpoint id must never be snapshotted twice, got " + finalSnapshotIds);
            for (int i = 1; i < finalSnapshotIds.size(); i++) {
                Assertions.assertTrue(
                        finalSnapshotIds.get(i) > finalSnapshotIds.get(i - 1),
                        "checkpoint ids must strictly increase, got " + finalSnapshotIds);
            }

            // 6. Cancelling must reach CANCELED and release the pipeline's fixed slot ownership.
            //    The wait is bounded so a cancellation regression fails here instead of blocking on
            //    the completion future until the CI job times out.
            jobProxy.cancelJob();
            awaitJobStatus(jobProxy, JobStatus.CANCELED);
            //    The snapshot count is sampled at the sink, so it establishes the absence of
            //    post-terminal sink snapshot requests; the coordinator-owned scheduler executor is
            //    not observed here.
            Thread.sleep(TERMINAL_SETTLE_MILLIS);
            int snapshotCallsAfterDrain = CheckpointSingleFlightSink.snapshotCallCount();
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(CHECKPOINT_INTERVAL_MS * 3));
            Assertions.assertEquals(
                    snapshotCallsAfterDrain,
                    CheckpointSingleFlightSink.snapshotCallCount(),
                    "the sink must not be asked for another snapshot after the job reached its "
                            + "terminal state, snapshots="
                            + CheckpointSingleFlightSink.snapshotIds());

            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                    finalNode.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
            Awaitility.await()
                    .atMost(CHECKPOINT_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            ownedSlotProfiles.containsKey(
                                                    new PipelineLocation(jobId, PIPELINE_ID)),
                                            "the cancelled job must release its fixed slots"));
        } finally {
            CheckpointSingleFlightSink.releaseFirstSnapshot();
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static SeaTunnelConfig getSeaTunnelConfig(
            String clusterName, String checkpointNamespace) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(TestUtils.getClusterName(clusterName));
        config.getEngineConfig().getHttpConfig().setEnabled(false);
        // Two slots for the single pipeline (split enumerator + one chained task group); four
        // fixed slots leave headroom and keep the fixed-slot resource model in effect.
        config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        config.getEngineConfig().getSlotServiceConfig().setSlotNum(4);

        CheckpointStorageConfig storage = new CheckpointStorageConfig();
        storage.setStorage("localfile");
        storage.setMaxRetainedCheckpoints(3);
        storage.setStoragePluginConfig(Collections.singletonMap("namespace", checkpointNamespace));
        config.getEngineConfig().getCheckpointConfig().setStorage(storage);
        return config;
    }

    private static PipelineCheckpointOverview pipelineOverview(SeaTunnelServer server, long jobId) {
        CheckpointOverview overview =
                server.getCheckpointMonitorService().getOverview(jobId).orElse(null);
        return overview == null ? null : overview.getPipelines().get(PIPELINE_ID);
    }

    private static long completedOccurrences(
            PipelineCheckpointOverview pipeline, long checkpointId) {
        return pipeline.getHistory().stream()
                .filter(entry -> entry.getCheckpointInfo().getCheckpointId() == checkpointId)
                .filter(
                        entry ->
                                entry.getCheckpointInfo().getStatus() == CheckpointStatus.COMPLETED)
                .count();
    }

    private static boolean hasCompletedAfter(
            PipelineCheckpointOverview pipeline, long checkpointId) {
        return pipeline.getHistory().stream()
                .map(CheckpointHistoryEntry::getCheckpointInfo)
                .anyMatch(
                        info ->
                                info.getCheckpointId() > checkpointId
                                        && info.getStatus() == CheckpointStatus.COMPLETED);
    }

    private interface PipelineAssertion {
        void check(PipelineCheckpointOverview pipeline);
    }

    /**
     * Waits for the job to reach {@code expectedStatus} within a bounded time.
     *
     * <p>{@link ClientJobProxy#waitForJobComplete()} blocks on the client completion future without
     * a timeout, so a regression that leaves it unresolved would stall this test until the external
     * CI timeout instead of failing it here.
     */
    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(TERMINAL_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "the job must reach " + expectedStatus));
    }

    private static void awaitOverview(
            SeaTunnelServer server, long jobId, PipelineAssertion assertion) {
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            PipelineCheckpointOverview pipeline = pipelineOverview(server, jobId);
                            Assertions.assertNotNull(
                                    pipeline, "the checkpoint overview is not published yet");
                            assertion.check(pipeline);
                        });
    }
}
