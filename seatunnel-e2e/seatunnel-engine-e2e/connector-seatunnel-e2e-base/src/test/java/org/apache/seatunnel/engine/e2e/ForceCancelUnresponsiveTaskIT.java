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
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.e2e.forcecancel.ForceCancelBlockingSink;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Guards the Zeta force-stop contract against a task that refuses the cooperative cancellation
 * handshake.
 *
 * <p><b>The contract.</b> A client force-stop ({@code ClientJobProxy#cancelJob(..., force=true)},
 * i.e. {@code CancelJobOperation} with {@code force=true} routed to {@code
 * CoordinatorService#stopJob}) exists precisely so an operator can end a job whose tasks do not
 * respond to the graceful {@code CANCELING} handshake. On a single in-process node with a fixed
 * two-slot pool ({@code dynamic-slot=false}, {@code slot-num=2}, WAIT scheduling), a RUNNING
 * streaming job whose sink writer deliberately swallows interrupts and stays parked inside {@code
 * write()} must therefore:
 *
 * <ol>
 *   <li>resolve the force-stop RPC, the live {@code JobMaster} completion future and the client
 *       completion future with {@code CANCELED} in a bounded window, while the task is still
 *       provably parked (the parked writer is released only after the assertions),
 *   <li>reach the terminal {@code CANCELED} state instead of staying in {@code CANCELING} or
 *       degrading to {@code UNKNOWABLE} after the coordinator releases the job master,
 *   <li>return every fixed slot it owned to the worker, proven both through {@code
 *       engine_ownedSlotProfilesIMap} and functionally by a second job that needs the same whole
 *       pool reaching {@code RUNNING},
 *   <li>stay {@code CANCELED} after the parked writer is finally released and its worker task group
 *       has been torn down.
 * </ol>
 *
 * <p><b>Why this is not already covered.</b> {@code
 * ClusterSeaTunnelEngineContainer#testForceStopJob} and {@code #testForceStopJobV2} only force-stop
 * a healthy, cooperative job through the REST API and then assert that it appears under finished
 * {@code CANCELED} jobs; the graceful cancel path that this fixture defeats would satisfy those
 * assertions unchanged, and they assert no resource release. {@code
 * CoordinatorServiceTest#testForceStopAbnormalSavepointJob} (the unit test shipped with the force
 * stop feature, #10075) hand-builds a {@code CoordinatorService} and never runs a real task that
 * refuses cancellation. The merged stop-path E2E {@code
 * SplitClusterPendingJobLifecycleFailoverIT#testSavepointStopReleasesFixedSlotsSoQueuedJobRuns}
 * covers stop-with-savepoint slot release with a cooperative task, a different cancel mode. No
 * existing test pins the property that makes force-stop meaningful: it must not wait for the
 * task-level cancel acknowledgement.
 *
 * <p><b>Regression it catches.</b> If {@code PhysicalVertex#forceStop} or the {@code stopJob}
 * branch starts waiting for the task terminal callback (for example by dropping the direct {@code
 * CANCELED} transition and relying on {@code cancel()}), this test fails at the terminal-state
 * assertion because the parked writer can never ack, and at the captured live {@code JobMaster}
 * completion future if the terminal state is only recorded in finished-job history. If the force
 * path stops returning the pipeline's fixed slots, the capacity probe stays {@code PENDING} and the
 * test fails there.
 */
@Slf4j
public class ForceCancelUnresponsiveTaskIT {

    private static final String BLOCKING_JOB_CONF =
            "force-cancel/stream_fake_to_force_cancel_blocking_sink.conf";

    private static final String CAPACITY_PROBE_CONF =
            "force-cancel/stream_fake_to_console_capacity_probe.conf";

    /**
     * Fixed worker capacity: one chained source+sink task group plus the FakeSource
     * split-enumerator coordinator group, so a single one-parallelism pipeline owns the whole pool.
     */
    private static final int SLOT_NUM = 2;

    private static final long STATE_TIMEOUT_SECONDS = 60;

    private static final long FORCE_CANCEL_TIMEOUT_SECONDS = 30;

    /**
     * Finished-job history retention for this fixture. The terminal record of the force-cancelled
     * job has to outlive the whole post-cancel phase, because the last status check reads it back
     * from finished-job history after the coordinator released the JobMaster. The module's {@code
     * seatunnel.yaml} retains finished jobs for one minute, which the combined deadlines below can
     * exceed on a slow CI run.
     */
    private static final int HISTORY_EXPIRE_MINUTES = 30;

    /**
     * How long the client completion request must stay outstanding before the force cancel is
     * issued. A request answered from finished-job history returns immediately, so an uninterrupted
     * window of this length while the job is still RUNNING shows that the request is attached to
     * the live JobMaster and not satisfied from history.
     */
    private static final long CLIENT_WAIT_ATTACH_SECONDS = 2;

    @Test
    public void testForceCancelStopsJobWhoseTaskIgnoresCooperativeCancel() throws Exception {
        String testCaseName = "testForceCancelStopsJobWhoseTaskIgnoresCooperativeCancel";
        String testClusterName = "ForceCancelUnresponsiveTaskIT_" + testCaseName;

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;

        try {
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);
            // The capacity probe is asserted to stay PENDING while the fixed slots are not
            // returned. With the module default (REJECT) an unschedulable job is failed before it
            // enters the pending queue instead, so WAIT is what makes that diagnostic meaningful.
            seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
            // Keep the finished-job record of the force-cancelled job readable for the whole
            // post-cancel phase instead of inheriting the module's one-minute retention.
            seaTunnelConfig.getEngineConfig().setHistoryJobExpireMinutes(HISTORY_EXPIRE_MINUTES);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            SeaTunnelClient finalEngineClient = engineClient;

            ForceCancelBlockingSink.reset();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(BLOCKING_JOB_CONF), jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = jobExecutionEnv.execute();
            long jobId = jobProxy.getJobId();

            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING,
                                            jobProxy.getJobStatus(),
                                            "The fixture job must reach RUNNING before the force "
                                                    + "cancel premise exists"));
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            ForceCancelBlockingSink.receivedRows() > 0,
                                            "The test sink must have received at least one row "
                                                    + "before it is armed"));

            // Premise for the slot-release assertion below: the fixture job owns the whole pool,
            // so the capacity probe can only start if the force-cancel path returns every slot.
            NodeEngineImpl nodeEngine = node.node.nodeEngine;
            Address workerAddress = node.node.address;
            SeaTunnelServer seaTunnelServer = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager =
                    seaTunnelServer.getCoordinatorService().getResourceManager();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            SLOT_NUM,
                                            resourceManager
                                                    .getRegisterWorker()
                                                    .get(workerAddress)
                                                    .getAssignedSlots()
                                                    .length,
                                            "The fixture job must own the whole fixed-slot pool "
                                                    + "before it is force-cancelled"));

            // Park the writer inside write(): from now on the task ignores interrupts and cannot
            // ack a graceful cancel, so only the force-stop path can end the job.
            ForceCancelBlockingSink.arm();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            ForceCancelBlockingSink.isBlockedInWrite(),
                                            "The sink writer must be parked inside write() before "
                                                    + "the force cancel is issued"));

            // Capture every completion observation before the force cancel, so none of them can
            // be satisfied from the finished-job history written once the JobMaster is gone.
            //
            // CoordinatorService#waitForJobComplete attaches a request that arrives while the job
            // is running to the live JobMaster completion future, and only falls back to finished
            // history once the JobMaster has been removed. A request issued after the removal
            // therefore cannot fail even if the force path stores CANCELED but never resolves the
            // live future, so the client wait has to be outstanding up front.
            JobMaster liveJobMaster = seaTunnelServer.getCoordinatorService().getJobMaster(jobId);
            Assertions.assertNotNull(
                    liveJobMaster,
                    "The fixture job must still be owned by a live JobMaster before the force "
                            + "cancel is issued");
            PassiveCompletableFuture<JobResult> liveCompletionFuture =
                    liveJobMaster.getJobMasterCompleteFuture();
            List<TaskGroupLocation> fixtureTaskGroups =
                    liveJobMaster.getPhysicalPlan().getPipelineList().stream()
                            .flatMap(pipeline -> pipeline.getPhysicalVertexList().stream())
                            .map(PhysicalVertex::getTaskGroupLocation)
                            .collect(Collectors.toList());
            Assertions.assertFalse(
                    fixtureTaskGroups.isEmpty(),
                    "The fixture job must expose the task group of its parked sink writer");
            TaskExecutionService taskExecutionService = seaTunnelServer.getTaskExecutionService();

            CountDownLatch clientWaitIssued = new CountDownLatch(1);
            CompletableFuture<JobStatus> completionFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                clientWaitIssued.countDown();
                                return jobProxy.waitForJobComplete();
                            });
            Assertions.assertTrue(
                    clientWaitIssued.await(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "The client completion request must be issued before the force cancel");
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .during(CLIENT_WAIT_ATTACH_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        completionFuture.isDone(),
                                        "The client completion request must stay outstanding "
                                                + "while the job is still RUNNING; an immediate "
                                                + "answer means it was served from finished-job "
                                                + "history instead of the live JobMaster");
                                Assertions.assertNotNull(
                                        seaTunnelServer.getCoordinatorService().getJobMaster(jobId),
                                        "The JobMaster must stay live while the client "
                                                + "completion request is outstanding");
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        jobProxy.getJobStatus(),
                                        "The fixture job must still be RUNNING while the client "
                                                + "completion request is outstanding");
                            });

            // 1. The force-stop request itself must resolve even though the task is parked.
            CompletableFuture<Void> forceCancelFuture =
                    CompletableFuture.runAsync(
                            () -> finalEngineClient.getJobClient().cancelJob(jobId, true));
            forceCancelFuture.get(FORCE_CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // 2. The job must converge to CANCELED while the writer is still parked, proving the
            // force path did not wait for the cooperative task acknowledgement.
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            jobProxy.getJobStatus(),
                                            "A force-cancelled job whose task ignores the "
                                                    + "cooperative handshake must still reach "
                                                    + "CANCELED, not stay in CANCELING"));
            Assertions.assertTrue(
                    ForceCancelBlockingSink.isBlockedInWrite(),
                    "The writer must still be parked when CANCELED is observed");
            Assertions.assertFalse(
                    ForceCancelBlockingSink.isReleased(),
                    "The writer must not have been released before the terminal-state assertions");

            // 3. The live JobMaster completion future captured before the force cancel must
            // resolve on the force path itself, still while the writer is parked. This is the
            // future an already attached client wait resolves through, and it is distinct from the
            // finished-job record written next to it, so a force path that stores CANCELED but
            // leaves the live future unresolved fails here.
            JobResult liveJobResult =
                    liveCompletionFuture.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    liveJobResult.getStatus(),
                    "The live JobMaster completion future of a force-cancelled job must resolve to "
                            + "CANCELED");
            Assertions.assertFalse(
                    ForceCancelBlockingSink.isReleased(),
                    "The writer must not have been released before the live completion future "
                            + "resolved");
            Assertions.assertFalse(
                    ForceCancelBlockingSink.hasExitedWrite(),
                    "The parked writer must not have left write() before the live completion "
                            + "future resolved");

            // 3b. The client completion request, outstanding since before the force cancel, must
            // resolve to the same terminal state while the writer is still held, proving the
            // client-visible future of a forced job is not left hanging.
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    completionFuture.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "The client completion future of a force-cancelled job must resolve to "
                            + "CANCELED");
            Assertions.assertFalse(
                    ForceCancelBlockingSink.hasExitedWrite(),
                    "The parked writer must still be held when the client completion future "
                            + "resolves");

            // 4. Every fixed slot must be back in the pool: the pipeline's slot ownership record
            // must be gone and a job that needs the whole pool must be able to start.
            IMap<PipelineLocation, Object> ownedSlotProfiles =
                    node.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            ownedSlotProfiles.keySet().stream()
                                                    .noneMatch(
                                                            location ->
                                                                    location.getJobId() == jobId),
                                            "The force-cancelled job must own no pipeline slots "
                                                    + "after it reaches CANCELED"));

            JobConfig probeJobConfig = new JobConfig();
            probeJobConfig.setName(testCaseName + "_capacity_probe");
            ClientJobProxy probeJobProxy =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(CAPACITY_PROBE_CONF),
                                    probeJobConfig,
                                    seaTunnelConfig)
                            .execute();
            long probeJobId = probeJobProxy.getJobId();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING,
                                            probeJobProxy.getJobStatus(),
                                            "A job that needs the whole fixed-slot pool must start "
                                                    + "after the force-cancelled job released it; "
                                                    + "a PENDING probe means slots leaked"));

            CompletableFuture<Void> probeCancelFuture =
                    CompletableFuture.runAsync(
                            () -> finalEngineClient.getJobClient().cancelJob(probeJobId, true));
            probeCancelFuture.get(FORCE_CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            probeJobProxy.getJobStatus(),
                                            "The capacity probe job must cancel cleanly"));

            // 5. Once the job is terminal the coordinator releases the JobMaster, after which the
            // client is served from finished-job history. That lookup must still answer CANCELED,
            // not UNKNOWABLE and not a stale live state.
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertNull(
                                            seaTunnelServer
                                                    .getCoordinatorService()
                                                    .getJobMaster(jobId),
                                            "The coordinator must release the JobMaster of the "
                                                    + "force-cancelled terminal job, so the next "
                                                    + "status read comes from finished history"));
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    jobProxy.getJobStatus(),
                    "The terminal state of the force-cancelled job must remain readable from "
                            + "finished-job history after the JobMaster is released");

            // 6. Finally let the parked writer go: wait for it to leave write() and for the worker
            // task-group teardown, then check that the already terminal job did not revive or
            // change state when the late worker callback ran.
            ForceCancelBlockingSink.release();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            ForceCancelBlockingSink.hasExitedWrite(),
                                            "Releasing the gate must make the parked write() call "
                                                    + "return"));
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            hasActiveTaskGroup(
                                                    taskExecutionService, fixtureTaskGroups),
                                            "The worker must tear down the task group of the "
                                                    + "released writer before the test concludes "
                                                    + "the job state; a lingering context means "
                                                    + "the teardown never ran"));
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    jobProxy.getJobStatus(),
                    "Releasing the previously parked writer must not move the job out of CANCELED");
        } finally {
            ForceCancelBlockingSink.release();
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    /** True while any of the given task groups still has an execution context on the worker. */
    private static boolean hasActiveTaskGroup(
            TaskExecutionService taskExecutionService, List<TaskGroupLocation> taskGroupLocations) {
        for (TaskGroupLocation location : taskGroupLocations) {
            try {
                if (taskExecutionService.getActiveExecutionContext(location) != null) {
                    return true;
                }
            } catch (TaskGroupContextNotFoundException e) {
                // No active deployment for this location is exactly what a torn-down task group
                // looks like on the worker.
            }
        }
        return false;
    }
}
