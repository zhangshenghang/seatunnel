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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.e2e.forcecancel.ForceCancelBlockingSink;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
 *   <li>resolve the force-stop RPC and the client completion future with {@code CANCELED} in a
 *       bounded window, while the task is still provably parked (the parked writer is released only
 *       after the assertions),
 *   <li>reach the terminal {@code CANCELED} state instead of staying in {@code CANCELING} or
 *       degrading to {@code UNKNOWABLE} after the coordinator releases the job master,
 *   <li>return every fixed slot it owned to the worker, proven both through {@code
 *       engine_ownedSlotProfilesIMap} and functionally by a second job that needs the same whole
 *       pool reaching {@code RUNNING},
 *   <li>stay {@code CANCELED} after the parked writer is finally released.
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
 * assertion because the parked writer can never ack. If the force path stops returning the
 * pipeline's fixed slots, the capacity probe stays {@code PENDING} and the test fails there.
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

            // 3. The client completion future must carry the same terminal state, not UNKNOWABLE.
            CompletableFuture<JobStatus> completionFuture =
                    CompletableFuture.supplyAsync(jobProxy::waitForJobComplete);
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    completionFuture.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "The client completion future of a force-cancelled job must resolve to "
                            + "CANCELED");

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

            // 5. Finally let the parked writer go: the already terminal job must not revive or
            // change state, and the task can now finish so the in-process node can shut down.
            ForceCancelBlockingSink.release();
            Thread.sleep(3000L);
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
}
