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

package org.apache.seatunnel.engine.e2e.cancelcheckpoint;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Regression guard for cancellation of a job whose checkpoint is unacknowledged: cancelling the job
 * must abort that checkpoint, never persist it even after the sink finally acknowledges it, drive
 * the job to CANCELED, and return the capacity for a replacement job.
 *
 * <p>Scenario: a streaming job runs on an in-process Zeta node. After a periodic checkpoint
 * completed, the test arms a test sink gate; the next checkpoint is frozen inside the sink writer's
 * {@code snapshotState(long)}, i.e. the barrier reached every task but the checkpoint is not
 * acknowledged and nothing is durable. The client then cancels the job while the writer is still
 * held.
 *
 * <p>Contract asserted here:
 *
 * <ol>
 *   <li>The accepted cancel moves the job to CANCELING while the checkpoint is still held and the
 *       abort reaches the checkpoint state machine instead of serializing behind an acknowledgement
 *       that may never come.
 *   <li>The named held checkpoint never becomes restorable, even after the held writer
 *       acknowledges: while the writer is parked no other checkpoint can complete, so the last
 *       checkpoint that completed before the hold stays the newest restorable candidate for the
 *       pipeline and the checkpoint directory of this job holds no file for the held id. Persisting
 *       it would turn rows that the cancelled run never committed into a restore point, i.e.
 *       duplicate data after a later restore.
 *   <li>The job reaches CANCELED once the held task returns and stays CANCELED (a late
 *       acknowledgement must not revive it or its checkpoint).
 *   <li>The cancelled job's capacity is usable again: the node runs a fixed slot pool ({@code
 *       dynamic-slot=false}) sized to exactly one job footprint, so a replacement job with the same
 *       footprint can only reach RUNNING after the cancelled job returned every slot it owned, and
 *       it releases the same bounded capacity when it is cancelled.
 * </ol>
 *
 * <p>The job's checkpoint storage is read through two different oracles on purpose: {@code
 * JobClient#getCheckpointData} reports the client's newest selectable checkpoint per pipeline (it
 * is not a storage inventory), while the checkpoint directory of this job is the actual ledger of
 * every stored checkpoint file.
 *
 * <p>Existing coverage does not reach this window. {@code CancelDuringMasterFailoverIT} crashes the
 * master during an accepted cancel but no checkpoint is in flight; {@code
 * SavepointBusySourceBarrierIT} covers stop-with-savepoint while the source is busy, with the
 * checkpoint completing; {@code
 * SplitClusterFaultToleranceIT#testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck} kills a
 * worker, not a checkpoint. No existing engine E2E cancels a RUNNING job while a periodic
 * checkpoint is provably unacknowledged at the sink and then inspects storage and capacity.
 */
@Slf4j
public class CancelWhileCheckpointInFlightIT {

    private static final String JOB_TEMPLATE_FILE =
            "cancel-checkpoint-in-flight/stream_fake_to_checkpoint_hold_sink.conf";
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /** Local file checkpoint storage option that names the storage root directory. */
    private static final String CHECKPOINT_NAMESPACE_KEY = "namespace";

    /** Suffix of a checkpoint file written by the local file checkpoint storage. */
    private static final String CHECKPOINT_FILE_SUFFIX = ".ser";

    /**
     * Slots one instance of the fixture job needs, derived from {@code PhysicalPlanGenerator}: one
     * source split enumerator coordinator plus one task group holding the fused source and sink
     * tasks of the single-parallelism pipeline. The test sink has no aggregated committer, so no
     * committer coordinator is created. The node runs a fixed pool of exactly this size, which is
     * what turns the replacement job into a capacity oracle.
     */
    private static final int JOB_SLOT_COUNT = 2;

    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 30;
    private static final long JOB_STATE_TIMEOUT_SECONDS = 180;
    private static final long HOLD_WINDOW_SECONDS = 30;
    private static final long CANCEL_TIMEOUT_SECONDS = 90;

    @Test
    public void testCancelResolvesAndAbortsHeldCheckpoint() throws Exception {
        String testCaseName = "testCancelResolvesAndAbortsHeldCheckpoint";
        String testClusterName = "CancelWhileCheckpointInFlightIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        String holdKey = testCaseName + "_" + System.nanoTime();
        String jobConfigFile = createJobConfigFile(testCaseName, holdKey);

        try {
            SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            HazelcastInstanceImpl finalNode = node;
            Awaitility.await()
                    .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            CoordinatorService coordinatorService = getCoordinatorService(node);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(jobConfigFile, jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = jobExecutionEnv.execute();
            long jobId = jobProxy.getJobId();

            awaitJobStatus(coordinatorService, jobId, JobStatus.RUNNING);
            assertSlotOwnership(node, jobId, JOB_SLOT_COUNT);
            JobClient jobClient = engineClient.createJobClient();
            long completedBeforeHold =
                    awaitCheckpoint(
                            jobClient, jobId, RestoreMode.CHECKPOINT, 0L, "before the hold");
            log.info(
                    "Job {} completed checkpoint {} before the hold was armed",
                    jobId,
                    completedBeforeHold);

            // Freeze the next checkpoint in the sink, after the barrier reached every task.
            CheckpointInFlightHoldGate.arm(holdKey);
            Awaitility.await()
                    .atMost(HOLD_WINDOW_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            CheckpointInFlightHoldGate.holdingWriters(holdKey) > 0,
                                            "a checkpoint must reach the sink and be held"));
            long heldCheckpointId = CheckpointInFlightHoldGate.heldCheckpointId(holdKey);
            // The hold is what makes the baseline stable: the sink writer cannot acknowledge the
            // only in-flight checkpoint, so the pipeline cannot complete another one. Reading the
            // last completed checkpoint here - not before arming - fences out a checkpoint that
            // completes in the window between the first observation and the arm; taken before the
            // hold, such a checkpoint would be a correct last restore point that the oracle would
            // reject.
            long lastCompletedBeforeHold =
                    latestCheckpointId(
                            jobClient, jobId, RestoreMode.CHECKPOINT, "last completed before hold");
            Assertions.assertTrue(
                    heldCheckpointId > lastCompletedBeforeHold,
                    "The held checkpoint "
                            + heldCheckpointId
                            + " must be newer than the last checkpoint completed before the hold ("
                            + lastCompletedBeforeHold
                            + ", first observed before arming: "
                            + completedBeforeHold
                            + ")");

            // Cancel while the checkpoint is unacknowledged. The cancel future is bounded so a
            // cancel that serializes behind the checkpoint surfaces as a test failure instead of
            // hanging the suite.
            CompletableFuture<Void> cancelFuture =
                    CompletableFuture.runAsync(() -> jobProxy.cancelJob());
            awaitCancelResolved(cancelFuture, jobId);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELING,
                                            coordinatorService.getJobStatus(jobId),
                                            "The accepted cancel must move the job to CANCELING"));
            Assertions.assertTrue(
                    CheckpointInFlightHoldGate.holdingWriters(holdKey) > 0,
                    "The cancel must reach the checkpoint while the sink still holds it");

            // The task is blocked in user code, so the terminal transition waits for it. What must
            // not wait is the checkpoint abort: once the writer acknowledges, the aborted
            // checkpoint must be discarded instead of completed and stored.
            CheckpointInFlightHoldGate.release(holdKey);
            Awaitility.await()
                    .atMost(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            coordinatorService.getJobStatus(jobId),
                                            "The cancelled job must terminate after its held task "
                                                    + "is released"));
            Awaitility.await()
                    .during(5, TimeUnit.SECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            coordinatorService.getJobStatus(jobId)));
            // Oracle 1: the client's newest selectable checkpoint per pipeline. The held checkpoint
            // must never become a restore candidate, and the last checkpoint completed before the
            // hold must stay the newest one.
            Set<Long> restorableCheckpoints =
                    latestCheckpointIds(jobClient, jobId, RestoreMode.CHECKPOINT);
            Assertions.assertEquals(
                    Set.of(lastCompletedBeforeHold),
                    restorableCheckpoints,
                    "The aborted checkpoint must never become restorable; the newest candidate for "
                            + "job "
                            + jobId
                            + " must stay the last checkpoint completed before the hold ("
                            + lastCompletedBeforeHold
                            + ") instead of "
                            + restorableCheckpoints);
            Assertions.assertFalse(
                    restorableCheckpoints.contains(heldCheckpointId),
                    "The aborted in-flight checkpoint "
                            + heldCheckpointId
                            + " became a restore candidate of job "
                            + jobId);
            // Oracle 2: the actual storage ledger of this job. The client API above only reports
            // the
            // newest candidate per pipeline, so it cannot prove that no file was written for the
            // aborted checkpoint; the checkpoint directory can.
            Set<Long> storedCheckpointFiles = storedCheckpointFileIds(seaTunnelConfig, jobId);
            Assertions.assertTrue(
                    storedCheckpointFiles.contains(lastCompletedBeforeHold),
                    "The checkpoint directory of job "
                            + jobId
                            + " must hold the checkpoint completed before the hold ("
                            + lastCompletedBeforeHold
                            + "), otherwise this storage oracle is not looking at the job's files: "
                            + storedCheckpointFiles);
            Assertions.assertFalse(
                    storedCheckpointFiles.contains(heldCheckpointId),
                    "The aborted in-flight checkpoint "
                            + heldCheckpointId
                            + " was persisted for job "
                            + jobId
                            + " even though its job was cancelled: "
                            + storedCheckpointFiles);
            // Every slot of the fixed pool must be back before the replacement is admitted.
            awaitSlotOwnership(node, jobId, 0);

            // The cancelled job's capacity must be usable again.
            String replacementHoldKey = testCaseName + "_replacement_" + System.nanoTime();
            String replacementConfigFile =
                    createJobConfigFile(testCaseName + "_replacement", replacementHoldKey);
            try {
                JobConfig replacementConfig = new JobConfig();
                replacementConfig.setName(testCaseName + "_replacement");
                ClientJobProxy replacement =
                        engineClient
                                .createExecutionContext(
                                        replacementConfigFile, replacementConfig, seaTunnelConfig)
                                .execute();
                long replacementId = replacement.getJobId();
                awaitJobStatus(coordinatorService, replacementId, JobStatus.RUNNING);
                // The pool is exactly one footprint wide, so the replacement can only reach
                // RUNNING after the cancelled job returned every slot it owned.
                assertSlotOwnership(node, replacementId, JOB_SLOT_COUNT);
                awaitCheckpoint(
                        jobClient, replacementId, RestoreMode.CHECKPOINT, 0L, "replacement job");
                replacement.cancelJob();
                awaitJobStatus(coordinatorService, replacementId, JobStatus.CANCELED);
                awaitSlotOwnership(node, replacementId, 0);
            } finally {
                FileUtils.deleteFile(replacementConfigFile);
            }
        } finally {
            CheckpointInFlightHoldGate.clear(holdKey);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null && node.getLifecycleService().isRunning()) {
                node.shutdown();
            }
            FileUtils.deleteFile(jobConfigFile);
        }
    }

    private static void awaitCancelResolved(CompletableFuture<Void> cancelFuture, long jobId) {
        try {
            cancelFuture.get(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            Assertions.fail(
                    "cancelJob() for job "
                            + jobId
                            + " did not resolve within "
                            + CANCEL_TIMEOUT_SECONDS
                            + "s while its checkpoint was held",
                    e);
        } catch (ExecutionException e) {
            Assertions.fail("cancelJob() for job " + jobId + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail("Interrupted while cancelling job " + jobId, e);
        }
    }

    /**
     * Returns the newest selectable checkpoint id per pipeline, as reported by the client. With a
     * single pipeline this is a singleton, and it is a restore-candidate selection rather than an
     * inventory of the stored checkpoint files.
     */
    private static Set<Long> latestCheckpointIds(
            JobClient jobClient, long jobId, RestoreMode restoreMode) {
        List<JobPipelineCheckpointData> checkpoints =
                jobClient.getCheckpointData(jobId, restoreMode);
        return checkpoints.stream()
                .map(JobPipelineCheckpointData::getCheckpointId)
                .collect(Collectors.toSet());
    }

    /**
     * Reads the id of the newest {@code restoreMode} checkpoint the client selects for the job.
     *
     * @throws AssertionError when the job has no such checkpoint
     */
    private static long latestCheckpointId(
            JobClient jobClient, long jobId, RestoreMode restoreMode, String description) {
        List<JobPipelineCheckpointData> checkpoints =
                jobClient.getCheckpointData(jobId, restoreMode);
        return checkpoints.stream()
                .max(Comparator.comparingLong(JobPipelineCheckpointData::getCheckpointId))
                .map(JobPipelineCheckpointData::getCheckpointId)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "No "
                                                + restoreMode
                                                + " checkpoint for "
                                                + description
                                                + " of job "
                                                + jobId
                                                + ": "
                                                + checkpoints));
    }

    /**
     * Reads the checkpoint ids the local file checkpoint storage actually holds for the job, i.e.
     * every stored checkpoint file rather than the newest one the client selects.
     */
    private static Set<Long> storedCheckpointFileIds(SeaTunnelConfig seaTunnelConfig, long jobId) {
        String namespace =
                seaTunnelConfig
                        .getEngineConfig()
                        .getCheckpointConfig()
                        .getStorage()
                        .getStoragePluginConfig()
                        .get(CHECKPOINT_NAMESPACE_KEY);
        Assertions.assertNotNull(
                namespace,
                "The engine config must configure the local file checkpoint storage namespace, "
                        + "otherwise the stored-checkpoint oracle cannot find the job's files");
        File[] files = new File(namespace, Long.toString(jobId)).listFiles();
        Set<Long> checkpointIds = new HashSet<>();
        if (files == null) {
            return checkpointIds;
        }
        for (File file : files) {
            String name = file.getName();
            if (!name.endsWith(CHECKPOINT_FILE_SUFFIX)) {
                continue;
            }
            String[] segments =
                    name.substring(0, name.length() - CHECKPOINT_FILE_SUFFIX.length()).split("-");
            // Stored files are named <writeMillis>-<random>-<pipelineId>-<checkpointId>.ser
            Assertions.assertEquals(
                    4, segments.length, "Unexpected checkpoint file name layout: " + name);
            checkpointIds.add(Long.parseLong(segments[3]));
        }
        return checkpointIds;
    }

    /**
     * Asserts slot ownership on the node's own slot pool, not on the master's heartbeated registry:
     * with a fixed pool this is the authoritative view of which job owns which slot.
     *
     * @param node the cluster member that also owns the worker slot pool
     * @param jobId the only job allowed to hold slots
     * @param expectedSlotCount {@link #JOB_SLOT_COUNT} while the job runs, zero once it released
     *     every slot
     */
    private static void assertSlotOwnership(
            HazelcastInstanceImpl node, long jobId, int expectedSlotCount) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        SlotProfile[] assignedSlots = server.getSlotService().getWorkerProfile().getAssignedSlots();
        if (expectedSlotCount == 0) {
            Assertions.assertEquals(
                    0,
                    assignedSlots.length,
                    "The fixed "
                            + JOB_SLOT_COUNT
                            + "-slot pool must be fully free after job "
                            + jobId
                            + " terminated, but slots are still assigned: "
                            + Arrays.toString(assignedSlots));
            return;
        }
        Assertions.assertEquals(
                expectedSlotCount,
                assignedSlots.length,
                "Job "
                        + jobId
                        + " must own all "
                        + expectedSlotCount
                        + " slots of the fixed pool: "
                        + Arrays.toString(assignedSlots));
        Set<Long> owners =
                Arrays.stream(assignedSlots)
                        .map(SlotProfile::getOwnerJobID)
                        .collect(Collectors.toSet());
        Assertions.assertEquals(
                Set.of(jobId),
                owners,
                "Every assigned slot must belong to job " + jobId + ": " + owners);
    }

    private static void awaitSlotOwnership(
            HazelcastInstanceImpl node, long jobId, int expectedSlotCount) {
        Awaitility.await()
                .atMost(JOB_STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertSlotOwnership(node, jobId, expectedSlotCount));
    }

    private static void awaitJobStatus(
            CoordinatorService coordinatorService, long jobId, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(JOB_STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        coordinatorService.getJobStatus(jobId),
                                        "Unexpected job status for job " + jobId));
    }

    /**
     * Waits until the latest checkpoint of the single pipeline for the given restore mode is above
     * {@code minExclusiveCheckpointId} and returns its id.
     */
    private static long awaitCheckpoint(
            JobClient jobClient,
            long jobId,
            RestoreMode restoreMode,
            long minExclusiveCheckpointId,
            String description) {
        Awaitility.await()
                .atMost(JOB_STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            long newest =
                                    latestCheckpointId(jobClient, jobId, restoreMode, description);
                            Assertions.assertTrue(
                                    newest > minExclusiveCheckpointId,
                                    "Waiting for a "
                                            + description
                                            + " checkpoint above id "
                                            + minExclusiveCheckpointId
                                            + " but the latest is "
                                            + newest);
                        });
        return latestCheckpointId(jobClient, jobId, restoreMode, description);
    }

    private static String createJobConfigFile(String testCaseName, String holdKey)
            throws Exception {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
        String jobConfigFile =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(JOB_TEMPLATE_FILE, valueMap, jobConfigFile);
        return jobConfigFile;
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        // A fixed pool of exactly one job footprint. The engine fixture's dynamic-slot:true would
        // hand the replacement job freshly allocated slots even when the cancelled job leaked its
        // ownership, so it could not prove that the capacity came back.
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(JOB_SLOT_COUNT);
        return seaTunnelConfig;
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
    }
}
