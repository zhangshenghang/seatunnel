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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * End-to-end guard for a savepoint restore that is still waiting for capacity when the active
 * master fails over.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A restore is admitted through a
 * different pending path than a fresh submission: the coordinator creates the restored {@link
 * JobMaster} immediately, but with {@code PendingSourceState.RESTORE} and without deploying any
 * pipeline until the resource pre-apply succeeds. A master switch tears that instance down ({@code
 * CoordinatorService#clearCoordinatorService}) and has to rebuild it purely from distributed state
 * ({@code runningJobInfoIMap} + persisted job state) through {@code
 * restoreAllRunningJobFromMasterNodeSwitch}. Several silent failures are possible in that
 * combination:
 *
 * <ul>
 *   <li>the restored pending entry is dropped instead of re-enqueued, so the job stays {@code
 *       PENDING} for ever with no scheduler entry (a "lost restore");
 *   <li>the recreated {@link JobMaster} is dispatched without rehydrating the savepoint state, so
 *       the job restarts from scratch, reuses checkpoint ids and overwrites the savepoint;
 *   <li>the restore is admitted twice (once by the old generation, once by the new one), consuming
 *       the savepoint twice or leaving a zombie pipeline writing alongside the live one;
 *   <li>the pending restore holds slices while it waits, starving the very capacity it needs.
 * </ul>
 *
 * <p><b>Why existing coverage does not reach it.</b> {@code SavepointRestoreCapacityAdmissionIT}
 * covers restore admission under slot starvation without a master switch. {@code
 * SavepointRestoreMasterFailoverIT} covers a restore that is already {@code RUNNING} before the
 * failover. {@code SavepointInFlightMasterFailoverIT} covers a savepoint request in flight during
 * the switch. {@code SplitClusterPendingJobLifecycleFailoverIT} covers pending <em>fresh</em> jobs.
 * No existing E2E fails over the active master while a restore is still queued behind occupied
 * slots, which is exactly where the rebuilt-from-distributed-state path and the savepoint
 * rehydration path meet.
 *
 * <p><b>Contract asserted here.</b>
 *
 * <ol>
 *   <li>A savepoint restore that cannot fit into the fixed slot pool is {@code PENDING}, owns no
 *       slice, holds no in-flight checkpoint and keeps the occupying job's slices untouched.
 *   <li>When the active master fails over, the pending restore survives as a single pending entry
 *       on the new master: the client and coordinator still report {@code PENDING}, the pending
 *       queue still contains exactly that job id, and a stability window shows no slice ownership
 *       and no checkpoint progress.
 *   <li>When the occupying job is cancelled, the pending restore is dispatched exactly once onto
 *       the freed slices, keeps the source job id and reaches {@code RUNNING}. This step also
 *       guards <a href="https://github.com/apache/seatunnel/issues/13536">#13536</a>: a job that a
 *       master switch leaves {@code PENDING} while its worker tasks are still deployed must not
 *       only drop its coordinator bookkeeping, which would leave the fixed pool permanently
 *       occupied.
 *   <li>The restored run resumes the savepoint checkpoint chain: the first periodic checkpoint the
 *       restored run actually completes is above the savepoint id, and the savepoint stays
 *       selectable, so the failover did not restart the job from scratch with a reset counter.
 *   <li>Cancelling the restored job returns the whole fixed pool to unassigned, so neither the
 *       pending restore nor the failover leaked admission or slot state.
 * </ol>
 *
 * <p>The {@code PENDING}-with-deployed-tasks cancellation boundary itself cannot stay pinned in
 * this scenario: the new master admits a job that still owns its slices as soon as the scheduler
 * reaches it, so whether the cancel lands inside that window depends on the pending queue order.
 * {@link #testPendingCancelAndStopOfRecoveredJobReleaseRetainedWorkerTasks()} pins that boundary
 * deterministically with a recovery gate the test owns.
 *
 * <p>Kept as its own class rather than added to an existing IT: the scenario needs a split cluster
 * (two master candidates plus one worker) whose active master is replaced mid-test, while the other
 * relevant classes either run a single hybrid node or never fail over at all.
 */
@Slf4j
public class PendingRestoreMasterFailoverIT {

    private static final String CONF_FILE =
            "pending-restore-failover/stream_fake_to_inmemory_pending_restore.conf";

    private static final String JOB_NAME = "pending-restore-master-failover";

    /**
     * Fixed pool of the single worker. One job occupies the whole pool: the split-enumerator task
     * group, the fused reader/writer task group and the sink task group of the {@code InMemory}
     * sink's aggregated committer.
     */
    private static final int SLOT_NUM = 3;

    private static final int SLOTS_PER_JOB = 3;

    /** The fixture has exactly one source-to-sink pipeline, so its id is always 1. */
    private static final int PIPELINE_ID = 1;

    /**
     * Page size for the checkpoint-history query. The monitor retains at most {@code
     * checkpoint.monitor.max-history-size} entries per pipeline, so a page this large returns every
     * entry it still keeps.
     */
    private static final int CHECKPOINT_HISTORY_LIMIT = 1000;

    private static final long STATE_TIMEOUT_SECONDS = 180;

    @Test
    public void testPendingSavepointRestoreSurvivesMasterFailoverAndRunsOnce() throws Exception {
        String testCaseName = "testPendingSavepointRestoreSurvivesMasterFailoverAndRunsOnce";
        String clusterName = "PendingRestoreMasterFailoverIT_" + testCaseName;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy sourceJob = null;
        ClientJobProxy occupyingJob = null;
        ClientJobProxy restoredJob = null;

        try {
            SeaTunnelConfig masterNode1Config = newSeaTunnelConfig(clusterName);
            SeaTunnelConfig masterNode2Config = newSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerNodeConfig = newSeaTunnelConfig(clusterName);

            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);
            Address workerAddress = workerNode.node.address;

            awaitClusterSize(workerNode, 3);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(clusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;
            CoordinatorService coordinatorService = coordinatorOf(activeMaster);
            ResourceManager resourceManager = coordinatorService.getResourceManager();
            CheckpointMonitorService checkpointMonitor =
                    serverOf(activeMaster).getCheckpointMonitorService();

            awaitWorkerAdvertisesWholePool(resourceManager, workerAddress);

            // ---- Phase 1: a checkpoint-enabled job owns the whole pool, then savepoints.
            sourceJob = submit(engineClient, masterNode1Config, "pending-restore-source");
            long jobId = sourceJob.getJobId();

            awaitStatus(coordinatorService, jobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, jobId, SLOTS_PER_JOB);
            awaitCompletedCheckpoints(checkpointMonitor, jobId, 1L);

            long savepointId = takeSavepoint(engineClient, jobClient, jobId);
            awaitStatus(coordinatorService, jobId, JobStatus.SAVEPOINT_DONE);

            // A restore is only meaningful once the savepointed run has fully left the running map;
            // otherwise submitJob deliberately treats the restore as a duplicate submission.
            awaitJobLeftRunningMapAndReleasedSlices(
                    coordinatorService, resourceManager, workerAddress, jobId);

            // ---- Phase 2: a second job occupies the whole fixed pool again.
            occupyingJob = submit(engineClient, masterNode1Config, "pending-restore-occupier");
            long occupyingJobId = occupyingJob.getJobId();

            awaitStatus(coordinatorService, occupyingJobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, occupyingJobId, SLOTS_PER_JOB);
            Assertions.assertEquals(
                    0,
                    workerProfile(resourceManager, workerAddress).getUnassignedSlots().length,
                    "the occupying job should fill the fixed pool");

            // ---- Phase 3: restore the savepoint while no capacity is left.
            JobConfig restoreJobConfig = new JobConfig();
            restoreJobConfig.setName(JOB_NAME);
            restoredJob =
                    engineClient
                            .restoreExecutionContext(
                                    TestUtils.getResource(CONF_FILE),
                                    restoreJobConfig,
                                    masterNode1Config,
                                    jobId)
                            .execute();
            final ClientJobProxy pendingRestore = restoredJob;

            Assertions.assertEquals(
                    jobId,
                    pendingRestore.getJobId(),
                    "a savepoint restore must keep the source job id");

            awaitStatus(coordinatorService, jobId, JobStatus.PENDING);
            awaitStatus(pendingRestore, JobStatus.PENDING);
            awaitPendingQueueContains(activeMaster, jobId);

            long completedBefore = completedCheckpoints(checkpointMonitor, jobId);
            long inProgressBefore = inProgressCheckpoints(checkpointMonitor, jobId);

            // Three scheduler retries (3s apart) without progress prove the job really waits for
            // capacity instead of being dispatched partially and failing later.
            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING, coordinatorService.getJobStatus(jobId));
                                Assertions.assertTrue(
                                        slicesOwnedBy(resourceManager, workerAddress, jobId)
                                                .isEmpty(),
                                        "a pending restore must not own any fixed slice");
                                Assertions.assertEquals(
                                        SLOTS_PER_JOB,
                                        slicesOwnedBy(
                                                        resourceManager,
                                                        workerAddress,
                                                        occupyingJobId)
                                                .size(),
                                        "the occupying job's slices must not be touched");
                                Assertions.assertEquals(
                                        completedBefore,
                                        completedCheckpoints(checkpointMonitor, jobId),
                                        "a pending restore must not complete checkpoints");
                                Assertions.assertEquals(
                                        inProgressBefore,
                                        inProgressCheckpoints(checkpointMonitor, jobId),
                                        "a pending restore must not start checkpoints");
                            });

            // ---- Phase 4: fail over the active master while the restore is still pending.
            activeMaster.shutdown();
            awaitCoordinatorActive(standbyMaster, 60);
            awaitClusterSize(workerNode, 2);

            HazelcastInstanceImpl newActiveMaster = standbyMaster;
            CoordinatorService newCoordinatorService = coordinatorOf(newActiveMaster);
            ResourceManager newResourceManager = newCoordinatorService.getResourceManager();
            CheckpointMonitorService newCheckpointMonitor =
                    serverOf(newActiveMaster).getCheckpointMonitorService();

            // Re-acquire proxies from a fresh JobClient: the old proxies were bound to the dead
            // master and would otherwise obscure whether the state really came from the new one.
            JobClient reconnectedJobClient = engineClient.createJobClient();
            ClientJobProxy restoreAfterFailover = reconnectedJobClient.getJobProxy(jobId);
            ClientJobProxy occupierAfterFailover = reconnectedJobClient.getJobProxy(occupyingJobId);

            awaitStatus(newCoordinatorService, jobId, JobStatus.PENDING);
            awaitStatus(restoreAfterFailover, JobStatus.PENDING);
            awaitPendingQueueContains(newActiveMaster, jobId);

            // The rebuilt pending entry must be a real JobMaster, not just an IMap status: a
            // coordinator that lost the entry would leave the status PENDING with no owner.
            awaitRestoredPendingJobMaster(newActiveMaster, jobId);

            // Hold a stability window before freeing capacity: the new master must neither dispatch
            // the restore early nor drop it while the scheduler retries. A duplicate admission
            // would also show up as a second slice owner or a started checkpoint here.
            long completedAfterFailover = completedCheckpoints(newCheckpointMonitor, jobId);
            long inProgressAfterFailover = inProgressCheckpoints(newCheckpointMonitor, jobId);
            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        newCoordinatorService.getJobStatus(jobId),
                                        "the restored pending job must survive the master switch");
                                Assertions.assertTrue(
                                        newCoordinatorService.getPendingJobQueue().contains(jobId),
                                        "the pending restore must stay in the new master's queue");
                                Assertions.assertTrue(
                                        slicesOwnedBy(newResourceManager, workerAddress, jobId)
                                                .isEmpty(),
                                        "a pending restore must own no slice after failover");
                                Assertions.assertEquals(
                                        SLOTS_PER_JOB,
                                        slicesOwnedBy(
                                                        newResourceManager,
                                                        workerAddress,
                                                        occupyingJobId)
                                                .size(),
                                        "the occupying job must keep its slices across failover");
                                Assertions.assertEquals(
                                        completedAfterFailover,
                                        completedCheckpoints(newCheckpointMonitor, jobId),
                                        "a pending restore must not complete checkpoints");
                                Assertions.assertEquals(
                                        inProgressAfterFailover,
                                        inProgressCheckpoints(newCheckpointMonitor, jobId),
                                        "a pending restore must not start checkpoints");
                            });

            // ---- Phase 5: free the pool; the pending restore must be dispatched onto it.
            // Test-owned fence for the first checkpoint of the restored run: it is taken while the
            // restore is still PENDING and therefore cannot have a checkpoint in flight.
            final long restoreAdmissionFenceMillis = System.currentTimeMillis();
            occupierAfterFailover.cancelJob();
            awaitStatus(newCoordinatorService, occupyingJobId, JobStatus.CANCELED);
            awaitStatus(newCoordinatorService, jobId, JobStatus.RUNNING);
            awaitOwnsSlices(newResourceManager, workerAddress, jobId, SLOTS_PER_JOB);
            Assertions.assertTrue(
                    slicesOwnedBy(newResourceManager, workerAddress, occupyingJobId).isEmpty(),
                    "the cancelled occupying job must own no slice");

            // ---- Phase 6: the restored run must resume the savepoint checkpoint chain.
            // The oracle is the first checkpoint the restored run actually completed after the
            // fence, not the latest checkpoint id: the latest-id view keeps the highest id ever
            // stored, so a run that reset its counter to 1 also passes an "id above the savepoint"
            // check once enough periodic checkpoints were taken, and the savepoint file is not
            // keyed by the checkpoint id alone (see AbstractCheckpointStorage), so the retained
            // savepoint does not prove the ids did not restart either.
            CheckpointInfo firstPostRestore =
                    awaitFirstCompletedCheckpointAfter(
                            newCheckpointMonitor,
                            jobId,
                            restoreAdmissionFenceMillis,
                            "periodic checkpoint after the restored pending job was dispatched");
            Assertions.assertTrue(
                    firstPostRestore.getCheckpointId() > savepointId,
                    "the restored run must continue after the savepoint id, savepoint="
                            + savepointId
                            + ", first periodic="
                            + firstPostRestore.getCheckpointId());
            Assertions.assertEquals(
                    CheckpointType.CHECKPOINT_TYPE,
                    firstPostRestore.getCheckpointType(),
                    "the first checkpoint after the restore must be a periodic checkpoint");

            // The savepoint stays selectable: it is the only durable recovery point of this job, so
            // the restored run may not replace the record selected for a SAVEPOINT restore. This is
            // a selectability check only: checkpoint files also carry a timestamp and a random
            // component, so a run that reused checkpoint ids would not necessarily overwrite the
            // savepoint and the check above is the one that observes the first post-restore id.
            List<JobPipelineCheckpointData> savepoints =
                    reconnectedJobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT);
            Assertions.assertEquals(
                    1,
                    savepoints.size(),
                    "exactly one savepoint must remain selectable: " + savepoints);
            Assertions.assertEquals(
                    savepointId,
                    savepoints.get(0).getCheckpointId(),
                    "the restored run must not replace the savepoint record");
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    savepoints.get(0).getCheckpointType(),
                    "the record selected for SAVEPOINT restore must still be a savepoint");

            // ---- Phase 7: the restored job ends and the fixed pool becomes fully free.
            restoredJob = restoreAfterFailover;
            restoredJob.cancelJob();
            restoredJob = null;
            awaitStatus(newCoordinatorService, jobId, JobStatus.CANCELED);
            awaitWholePoolUnassigned(newResourceManager, workerAddress);
        } finally {
            cancelQuietly(sourceJob);
            cancelQuietly(occupyingJob);
            cancelQuietly(restoredJob);
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
        }
    }

    /**
     * Deterministic guard for the cancel/stop branch a master switch leaves behind.
     *
     * <p>{@code CoordinatorService#restoreJobFromMasterActiveSwitch} rebuilds a running job's
     * {@code JobMaster} from distributed state and re-enters it as {@link JobStatus#PENDING} while
     * the worker still executes the task groups the previous master deployed and still owns their
     * fixed slices ({@code JobMaster#getReusableSlot} deliberately reuses them). Whether the
     * pending scheduler admits such a job before a cancel arrives depends on the pending queue
     * order, so {@link #testPendingSavepointRestoreSurvivesMasterFailoverAndRunsOnce()} cannot keep
     * that window open. This test pins the boundary with a recovery gate the test owns: the job is
     * forced back to {@code PENDING} and the plan, pipeline and vertex state processes are stopped,
     * exactly as a plan rebuilt from distributed state has never started them, while its worker
     * task groups and slices are proven still active before the request.
     *
     * <p>Both the normal cancel path and the force-stop path must terminate those retained worker
     * executions and release the slices, and they must not report the slices as free while the
     * execution holding them is still alive: a replacement job queued on the same fixed pool may
     * only be admitted onto slices no retained task group is using any more.
     */
    @Test
    public void testPendingCancelAndStopOfRecoveredJobReleaseRetainedWorkerTasks()
            throws Exception {
        String testCaseName = "testPendingCancelAndStopOfRecoveredJobReleaseRetainedWorkerTasks";
        String clusterName = "PendingRestoreMasterFailoverIT_" + testCaseName;

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        try {
            SeaTunnelConfig masterNodeConfig = newSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerNodeConfig = newSeaTunnelConfig(clusterName);

            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);
            Address workerAddress = workerNode.node.address;

            awaitClusterSize(workerNode, 2);
            awaitCoordinatorActive(masterNode, 60);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(clusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            CoordinatorService coordinatorService = coordinatorOf(masterNode);
            ResourceManager resourceManager = coordinatorService.getResourceManager();
            TaskExecutionService workerTaskExecutionService =
                    serverOf(workerNode).getTaskExecutionService();
            awaitWorkerAdvertisesWholePool(resourceManager, workerAddress);

            // ---- A job that a master switch left PENDING, cancelled through the normal path.
            assertRecoveredPendingTerminationFreesRetainedExecution(
                    engineClient,
                    masterNodeConfig,
                    coordinatorService,
                    resourceManager,
                    workerTaskExecutionService,
                    workerAddress,
                    "recovered-pending-cancel",
                    "cancel",
                    ClientJobProxy::cancelJob);

            // ---- The force-stop path (CoordinatorService#stopJob) must take the same branch.
            assertRecoveredPendingTerminationFreesRetainedExecution(
                    engineClient,
                    masterNodeConfig,
                    coordinatorService,
                    resourceManager,
                    workerTaskExecutionService,
                    workerAddress,
                    "recovered-pending-stop",
                    "stop",
                    job -> coordinatorService.stopJob(job.getJobId()).join());
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    private static SeaTunnelConfig newSeaTunnelConfig(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(clusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        // Fixed slices make slot ownership observable; WAIT is what turns "no capacity yet" into
        // PENDING instead of an immediate failure, which is the admission contract here.
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        return seaTunnelConfig;
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient, SeaTunnelConfig seaTunnelConfig, String jobName)
            throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        return engineClient
                .createExecutionContext(
                        TestUtils.getResource(CONF_FILE), jobConfig, seaTunnelConfig)
                .execute();
    }

    /**
     * Takes a stop-with-savepoint and returns the persisted savepoint id of the single pipeline.
     * The client call blocks until the job reaches a terminal state, so it is bounded here to keep
     * a stuck {@code DOING_SAVEPOINT} from hanging the suite.
     */
    private static long takeSavepoint(SeaTunnelClient engineClient, JobClient jobClient, long jobId)
            throws Exception {
        CompletableFuture<Void> savepoint =
                CompletableFuture.runAsync(() -> engineClient.savePointJob(jobId));
        savepoint.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        AtomicReference<JobPipelineCheckpointData> persisted = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT);
                            JobPipelineCheckpointData newest =
                                    checkpoints.stream()
                                            .filter(data -> data.getPipelineId() == PIPELINE_ID)
                                            .max(
                                                    Comparator.comparingLong(
                                                            JobPipelineCheckpointData
                                                                    ::getCheckpointId))
                                            .orElse(null);
                            Assertions.assertNotNull(
                                    newest,
                                    "the savepoint must be persisted before the restore: "
                                            + checkpoints);
                            Assertions.assertEquals(
                                    CheckpointType.SAVEPOINT_TYPE,
                                    newest.getCheckpointType(),
                                    "the persisted checkpoint must be a savepoint");
                            persisted.set(newest);
                        });
        return persisted.get().getCheckpointId();
    }

    private static void awaitJobLeftRunningMapAndReleasedSlices(
            CoordinatorService coordinatorService,
            ResourceManager resourceManager,
            Address workerAddress,
            long jobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(
                                    coordinatorService.getJobMaster(jobId),
                                    "the savepointed run must leave the running map");
                            Assertions.assertTrue(
                                    slicesOwnedBy(resourceManager, workerAddress, jobId).isEmpty(),
                                    "the savepointed run must release every fixed slice");
                            Assertions.assertEquals(
                                    SLOT_NUM,
                                    workerProfile(resourceManager, workerAddress)
                                            .getUnassignedSlots()
                                            .length);
                        });
    }

    private static void awaitWorkerAdvertisesWholePool(
            ResourceManager resourceManager, Address workerAddress) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            WorkerProfile profile = workerProfile(resourceManager, workerAddress);
                            Assertions.assertNotNull(profile, "worker should register");
                            Assertions.assertEquals(
                                    SLOT_NUM,
                                    profile.getUnassignedSlots().length,
                                    "worker should advertise all fixed slices");
                        });
    }

    private static void awaitRestoredPendingJobMaster(
            HazelcastInstanceImpl activeMaster, long jobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster = coordinatorOf(activeMaster).getJobMaster(jobId);
                            Assertions.assertNotNull(
                                    jobMaster,
                                    "the new master must rebuild a JobMaster for the pending restore");
                            Assertions.assertEquals(
                                    JobStatus.PENDING,
                                    jobMaster.getJobStatus(),
                                    "the rebuilt JobMaster must still be pending");
                        });
    }

    /**
     * Returns the first checkpoint of {@code jobId}/{@code PIPELINE_ID} that completed after {@code
     * fenceMillis}, ordered by trigger timestamp.
     *
     * <p>The latest-checkpoint view cannot prove what the restored run produced first: it keeps the
     * highest id ever stored, so a run that reset its counter to 1 satisfies an "id above the
     * savepoint" check again as soon as enough periodic checkpoints were taken. The checkpoint
     * monitor keeps every completion with its trigger timestamp, so a fence the test owns observes
     * the first checkpoint the restored run actually produced.
     *
     * <p>The monitor is read in process instead of through the client checkpoint-history request:
     * that request runs on an operation thread which resolves the distributed overview map, and
     * blocking there can stall the query itself while a master switch settles, which would turn
     * this oracle into a client timeout instead of an assertion.
     */
    private static CheckpointInfo awaitFirstCompletedCheckpointAfter(
            CheckpointMonitorService checkpointMonitor,
            long jobId,
            long fenceMillis,
            String description) {
        AtomicReference<CheckpointInfo> firstAfterFence = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            CheckpointInfo earliest =
                                    checkpointMonitor
                                            .getHistory(
                                                    jobId,
                                                    PIPELINE_ID,
                                                    CHECKPOINT_HISTORY_LIMIT,
                                                    CheckpointStatus.COMPLETED)
                                            .stream()
                                            .map(CheckpointHistoryEntry::getCheckpointInfo)
                                            .filter(info -> info != null)
                                            .filter(
                                                    info ->
                                                            info.getTriggerTimestamp()
                                                                    >= fenceMillis)
                                            .min(
                                                    Comparator.comparingLong(
                                                            CheckpointInfo::getTriggerTimestamp))
                                            .orElse(null);
                            Assertions.assertNotNull(
                                    earliest,
                                    "no " + description + " completed after the restore fence");
                            firstAfterFence.set(earliest);
                        });
        return firstAfterFence.get();
    }

    private static void awaitStatus(
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

    private static void awaitStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        jobProxy.getJobStatus(),
                                        "client status of job " + jobProxy.getJobId()));
    }

    private static void awaitOwnsSlices(
            ResourceManager resourceManager, Address workerAddress, long jobId, int expected) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        slicesOwnedBy(resourceManager, workerAddress, jobId).size(),
                                        "slices owned by job "
                                                + jobId
                                                + ": "
                                                + slicesByJob(resourceManager, workerAddress)));
    }

    private static void awaitCompletedCheckpoints(
            CheckpointMonitorService checkpointMonitor, long jobId, long expectedAtLeast) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        completedCheckpoints(checkpointMonitor, jobId)
                                                >= expectedAtLeast,
                                        "completed checkpoints of job "
                                                + jobId
                                                + ": "
                                                + completedCheckpoints(checkpointMonitor, jobId)));
    }

    private static void awaitPendingQueueContains(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            CoordinatorService coordinatorService = coordinatorOf(masterNode);
                            Assertions.assertTrue(
                                    coordinatorService.isCoordinatorActive(),
                                    "coordinator should be active when asserting the pending queue");
                            Assertions.assertTrue(
                                    coordinatorService.getPendingJobQueue().contains(jobId),
                                    "the pending queue should contain job "
                                            + jobId
                                            + ", queue size "
                                            + coordinatorService.getPendingJobCount());
                        });
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, node.getCluster().getMembers().size()));
    }

    private static void awaitCoordinatorActive(
            HazelcastInstanceImpl masterNode, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(masterNode.getLifecycleService().isRunning());
                            Assertions.assertTrue(
                                    isCoordinatorActive(masterNode),
                                    "standby master should become active after failover");
                        });
    }

    private static HazelcastInstanceImpl waitAndFindActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        AtomicReference<HazelcastInstanceImpl> activeMaster = new AtomicReference<>();
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            if (isCoordinatorActive(masterNode1)) {
                                activeMaster.set(masterNode1);
                            } else if (isCoordinatorActive(masterNode2)) {
                                activeMaster.set(masterNode2);
                            }
                            Assertions.assertNotNull(
                                    activeMaster.get(),
                                    "should find an active master after coordinator initialization");
                        });
        return activeMaster.get();
    }

    private static boolean isCoordinatorActive(HazelcastInstanceImpl node) {
        if (node == null || !node.getLifecycleService().isRunning()) {
            return false;
        }
        try {
            return coordinatorOf(node).isCoordinatorActive();
        } catch (SeaTunnelEngineException e) {
            return false;
        }
    }

    private static CoordinatorService coordinatorOf(HazelcastInstanceImpl node) {
        return serverOf(node).getCoordinatorService();
    }

    private static SeaTunnelServer serverOf(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    private static long completedCheckpoints(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(pipeline -> pipeline.getCounts().getCompleted())
                                        .sum())
                .orElse(0L);
    }

    private static long inProgressCheckpoints(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(
                                                pipeline ->
                                                        pipeline.getCounts().getInProgress()
                                                                + pipeline.getInProgress().size())
                                        .sum())
                .orElse(0L);
    }

    private static WorkerProfile workerProfile(
            ResourceManager resourceManager, Address workerAddress) {
        return resourceManager.getRegisterWorker().get(workerAddress);
    }

    private static Map<Long, Set<Integer>> slicesByJob(
            ResourceManager resourceManager, Address workerAddress) {
        Map<Long, Set<Integer>> slices = new TreeMap<>();
        WorkerProfile profile = workerProfile(resourceManager, workerAddress);
        if (profile == null) {
            return slices;
        }
        for (SlotProfile slot : profile.getAssignedSlots()) {
            slices.computeIfAbsent(slot.getOwnerJobID(), ignored -> new TreeSet<>())
                    .add(slot.getSlotID());
        }
        return slices;
    }

    private static Set<Integer> slicesOwnedBy(
            ResourceManager resourceManager, Address workerAddress, long jobId) {
        return slicesByJob(resourceManager, workerAddress)
                .getOrDefault(jobId, Collections.emptySet());
    }

    /**
     * Drives one job through the recovered-pending termination boundary: admit it onto the whole
     * fixed pool, pin the state a master switch leaves behind, and require the termination request
     * to stop the retained worker executions before the freed slices may be reused.
     */
    private static void assertRecoveredPendingTerminationFreesRetainedExecution(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            CoordinatorService coordinatorService,
            ResourceManager resourceManager,
            TaskExecutionService workerTaskExecutionService,
            Address workerAddress,
            String jobName,
            String action,
            Consumer<ClientJobProxy> terminationRequest)
            throws Exception {
        ClientJobProxy targetJob = submit(engineClient, seaTunnelConfig, jobName);
        long targetJobId = targetJob.getJobId();
        ClientJobProxy replacementJob = null;
        try {
            awaitStatus(coordinatorService, targetJobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, targetJobId, SLOTS_PER_JOB);

            JobMaster targetJobMaster = coordinatorService.getJobMaster(targetJobId);
            Assertions.assertNotNull(
                    targetJobMaster,
                    "the running job must be tracked by the coordinator before it is pinned");
            // The retained execution must be running on the worker before the gate: a task group
            // that is still being deployed has no worker execution context yet, and a master switch
            // can only leave behind task groups the previous master actually deployed.
            List<TaskGroupLocation> deployedTaskGroups = taskGroupsOf(targetJobMaster);
            Assertions.assertFalse(
                    deployedTaskGroups.isEmpty(),
                    "the job must own at least one deployed task group before it is pinned");
            awaitAllExecutionsActive(workerTaskExecutionService, deployedTaskGroups);

            List<TaskGroupLocation> retainedTaskGroups =
                    pinRecoveredPendingRecoveryState(coordinatorService, targetJobMaster);

            // The gate must produce the recovered shape, otherwise the assertions below would be
            // satisfied by a plain pending job that owns nothing.
            Assertions.assertEquals(
                    SLOTS_PER_JOB,
                    slicesOwnedBy(resourceManager, workerAddress, targetJobId).size(),
                    "the pinned job must still own the whole fixed pool");
            Assertions.assertEquals(
                    deployedTaskGroups,
                    retainedTaskGroups,
                    "the pinned job must keep exactly the task groups it had deployed");
            Assertions.assertTrue(
                    isAnyExecutionActive(workerTaskExecutionService, retainedTaskGroups),
                    "the pinned job must still execute on the worker before the "
                            + action
                            + " request: "
                            + retainedTaskGroups);

            // The replacement is queued on the same fixed pool before the request: it may only be
            // admitted onto slices that no retained execution is using any more.
            replacementJob = submit(engineClient, seaTunnelConfig, jobName + "-replacement");
            long replacementJobId = replacementJob.getJobId();
            awaitStatus(coordinatorService, replacementJobId, JobStatus.PENDING);

            terminationRequest.accept(targetJob);

            long deadlineMillis =
                    System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(STATE_TIMEOUT_SECONDS);
            while (System.currentTimeMillis() < deadlineMillis) {
                boolean retainedExecutionActive =
                        isAnyExecutionActive(workerTaskExecutionService, retainedTaskGroups);
                boolean slicesReportedFree =
                        slicesOwnedBy(resourceManager, workerAddress, targetJobId).isEmpty();
                Assertions.assertFalse(
                        retainedExecutionActive && slicesReportedFree,
                        action
                                + " reported the fixed slices of job "
                                + targetJobId
                                + " as free while its retained worker task groups were still executing: "
                                + retainedTaskGroups);
                if (!retainedExecutionActive
                        && slicesReportedFree
                        && JobStatus.RUNNING.equals(
                                coordinatorService.getJobStatus(replacementJobId))) {
                    break;
                }
                Thread.sleep(100);
            }

            // The termination path may delete the pending job's state maps once it is terminal, so
            // only the absence of a non-terminal state is asserted here; the durable evidence is
            // the worker execution and slice ownership checked below.
            JobStatus targetFinalStatus = targetJobMaster.getJobStatus();
            Assertions.assertTrue(
                    targetFinalStatus == null || JobStatus.CANCELED.equals(targetFinalStatus),
                    action
                            + " must not leave the pending job in "
                            + targetFinalStatus
                            + " after terminating it");
            Assertions.assertFalse(
                    isAnyExecutionActive(workerTaskExecutionService, retainedTaskGroups),
                    action
                            + " left worker task groups running for job "
                            + targetJobId
                            + ": "
                            + retainedTaskGroups);
            Assertions.assertTrue(
                    slicesOwnedBy(resourceManager, workerAddress, targetJobId).isEmpty(),
                    action + " left fixed slices owned by job " + targetJobId);

            // Capacity is only usable again because the retained execution really terminated.
            awaitStatus(coordinatorService, replacementJobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, replacementJobId, SLOTS_PER_JOB);

            // Ending the replacement must return the whole fixed pool, so the termination path did
            // not leave the cancelled job's ownership behind.
            replacementJob.cancelJob();
            awaitStatus(coordinatorService, replacementJobId, JobStatus.CANCELED);
            replacementJob = null;
            awaitWholePoolUnassigned(resourceManager, workerAddress);
        } finally {
            cancelQuietly(targetJob);
            cancelQuietly(replacementJob);
        }
    }

    /**
     * Recovery gate: reproduces the state {@code
     * CoordinatorService#restoreJobFromMasterActiveSwitch} leaves behind for a job whose worker
     * tasks survived the switch.
     *
     * <p>The rebuilt {@code JobMaster} is forced back to {@link JobStatus#PENDING} and its plan,
     * pipeline and vertex state processes are stopped, exactly as a plan rebuilt from distributed
     * state has never started them. The worker task groups and the persisted slice ownership are
     * deliberately left untouched, so the job is pending only from the coordinator's point of view,
     * which is the state the cancel and force-stop paths have to handle.
     *
     * @return the task groups the job still has deployed on the worker
     */
    private static List<TaskGroupLocation> pinRecoveredPendingRecoveryState(
            CoordinatorService coordinatorService, JobMaster jobMaster) {
        PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
        List<SubPlan> pipelines = physicalPlan.getPipelineList();
        pipelines.forEach(SubPlan::stopSubPlanStateProcess);
        pipelines.forEach(
                pipeline -> {
                    pipeline.getCoordinatorVertexList().forEach(PhysicalVertex::stopPhysicalVertex);
                    pipeline.getPhysicalVertexList().forEach(PhysicalVertex::stopPhysicalVertex);
                });
        physicalPlan.stopJobStateProcess();
        physicalPlan.updateJobState(JobStatus.PENDING);
        Assertions.assertEquals(
                JobStatus.PENDING,
                coordinatorService.getJobStatus(jobMaster.getJobId()),
                "the recovery gate must leave the job pending");
        return taskGroupsOf(jobMaster);
    }

    /** Task groups the plan owns, coordinator groups first. */
    private static List<TaskGroupLocation> taskGroupsOf(JobMaster jobMaster) {
        List<TaskGroupLocation> taskGroups = new ArrayList<>();
        jobMaster
                .getPhysicalPlan()
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            pipeline.getCoordinatorVertexList()
                                    .forEach(
                                            vertex ->
                                                    taskGroups.add(vertex.getTaskGroupLocation()));
                            pipeline.getPhysicalVertexList()
                                    .forEach(
                                            vertex ->
                                                    taskGroups.add(vertex.getTaskGroupLocation()));
                        });
        return taskGroups;
    }

    /**
     * Waits until the worker executes every given task group. A task group that is still being
     * deployed has no execution context yet, so gating before this point would cancel nothing and
     * would not reproduce the retained-execution boundary.
     */
    private static void awaitAllExecutionsActive(
            TaskExecutionService taskExecutionService, List<TaskGroupLocation> taskGroups) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                taskGroups.forEach(
                                        taskGroup ->
                                                Assertions.assertNotNull(
                                                        taskExecutionService
                                                                .getActiveExecutionContext(
                                                                        taskGroup),
                                                        "the job must execute its worker task group "
                                                                + taskGroup
                                                                + " before the recovery gate")));
    }

    /** True when the worker still executes at least one of the given task groups. */
    private static boolean isAnyExecutionActive(
            TaskExecutionService taskExecutionService, List<TaskGroupLocation> taskGroups) {
        for (TaskGroupLocation taskGroup : taskGroups) {
            try {
                if (taskExecutionService.getActiveExecutionContext(taskGroup) != null) {
                    return true;
                }
            } catch (TaskGroupContextNotFoundException e) {
                // The worker already dropped this deployment.
            }
        }
        return false;
    }

    private static void awaitWholePoolUnassigned(
            ResourceManager resourceManager, Address workerAddress) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            WorkerProfile profile = workerProfile(resourceManager, workerAddress);
                            Assertions.assertNotNull(profile, "the worker must stay registered");
                            Assertions.assertEquals(
                                    0,
                                    profile.getAssignedSlots().length,
                                    "no slice may stay assigned after every job ended: "
                                            + slicesByJob(resourceManager, workerAddress));
                            Assertions.assertEquals(
                                    SLOT_NUM,
                                    profile.getUnassignedSlots().length,
                                    "the whole fixed pool must be unassigned again");
                        });
    }

    private static void cancelQuietly(ClientJobProxy jobProxy) {
        if (jobProxy == null) {
            return;
        }
        try {
            jobProxy.cancelJob();
        } catch (Throwable e) {
            log.info(
                    "Ignoring failure while cancelling job {} during cleanup",
                    jobProxy.getJobId(),
                    e);
        }
    }
}
