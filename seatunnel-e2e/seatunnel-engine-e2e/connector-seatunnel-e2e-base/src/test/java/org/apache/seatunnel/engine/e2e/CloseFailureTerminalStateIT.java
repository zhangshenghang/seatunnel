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
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.e2e.terminalclose.CloseFailingSink;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
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
 * Guards the Zeta terminal-close contract: a failure of the sink's last {@code close()} call must
 * fail the job, never be reported as a successful finish.
 *
 * <p><b>The contract.</b> On a single in-process node with a fixed two-slot pool ({@code
 * dynamic-slot=false}, {@code slot-num=2}, WAIT scheduling), a one-parallelism BATCH job whose sink
 * accepts every row and then throws from {@link org.apache.seatunnel.api.sink.SinkWriter#close()}
 * must:
 *
 * <ol>
 *   <li>resolve its client completion future as {@code FAILED}, not {@code FINISHED}, and carry the
 *       close failure on the client-visible {@link JobResult},
 *   <li>deliver every source row exactly once: with {@code job.retry.times=0} the failed close must
 *       not silently redeploy the pipeline, so the sink sees exactly the configured row count,
 *   <li>stay {@code FAILED} as a terminal state after the coordinator released the job master
 *       instead of degrading to {@code UNKNOWABLE} or flipping to {@code FINISHED},
 *   <li>return every fixed slot it owned to the worker (no entry in {@code
 *       engine_ownedSlotProfilesIMap}, zero assigned slots) so a replacement job with the same
 *       two-slot footprint reaches {@code FINISHED}.
 * </ol>
 *
 * <p><b>Why this is not already covered.</b> All existing sink-failure coverage fails the writer
 * earlier, in {@code write()} or {@code open()}: {@code ClusterIT#testTaskGroupErrorMsgLost},
 * {@code JobClientJobProxyIT#testMultiTableSinkFailedWithThrowable} and the {@code
 * SinkErrorToMysqlIT} family (which additionally installs an error handler and therefore routes
 * rows instead of failing the job). {@code SplitClusterWorkerRestartAfterFinalCheckpointIT} parks
 * close() on a gate but then releases it normally, and {@code FinalCheckpointCloseHoldSinkWriter}
 * never throws. No existing engine E2E asserts the terminal-close failure path, even though a close
 * exception is the one failure that arrives after the pipeline already consumed all records.
 *
 * <p><b>Regression it catches.</b> If the close handshake swallows the writer exception (for
 * example to keep a future cancellation fix from hanging, or because the deferrable close outcome
 * is only logged), the job is reported {@code FINISHED} and this test fails at the first assertion.
 * If the failure instead leaks the pipeline's fixed slots, the capacity probe stays {@code PENDING}
 * and the test fails there.
 */
@Slf4j
public class CloseFailureTerminalStateIT {

    private static final String FAILING_JOB_CONF =
            "terminal-close/batch_fake_to_close_failing_sink.conf";

    private static final String CAPACITY_PROBE_CONF =
            "terminal-close/batch_fake_to_console_capacity_probe.conf";

    /** Fixed worker capacity: the one-parallelism batch job and the probe share this pool. */
    private static final int SLOT_NUM = 2;

    /** Rows configured in the failing job conf; the sink must see each of them exactly once. */
    private static final long EXPECTED_ROWS = 50L;

    private static final long STATE_TIMEOUT_SECONDS = 60;

    @Test
    public void testSinkCloseFailureFailsJobAndReleasesCapacity() throws Exception {
        String testCaseName = "testSinkCloseFailureFailsJobAndReleasesCapacity";
        String testClusterName = "CloseFailureTerminalStateIT_" + testCaseName;

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

            CloseFailingSink.reset();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(FAILING_JOB_CONF), jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = jobExecutionEnv.execute();
            long jobId = jobProxy.getJobId();

            // 1. A failed terminal close must resolve the job as FAILED, never as FINISHED.
            CompletableFuture<JobStatus> completionFuture =
                    CompletableFuture.supplyAsync(jobProxy::waitForJobComplete);
            JobStatus terminalStatus =
                    completionFuture.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    JobStatus.FAILED,
                    terminalStatus,
                    "A job whose sink close() throws must end FAILED, not report a successful "
                            + "finish");

            // The client-visible result must carry the close failure instead of an empty error.
            JobResult jobResult = jobProxy.getJobResultCache();
            Assertions.assertEquals(JobStatus.FAILED, jobResult.getStatus());
            Assertions.assertNotNull(
                    jobResult.getError(), "The failed job result must carry error info");
            Assertions.assertTrue(
                    jobResult.getError().contains(CloseFailingSink.CLOSE_FAILURE_MESSAGE),
                    "The job result must surface the sink close failure, but was: "
                            + jobResult.getError());
            log.warn(
                    "Observed terminal close failure on the client job result: {}",
                    jobResult.getError());

            // 2. Exactly one deployment: every source row was written once and only the close
            // failed. A silent retry would double the observed rows.
            Assertions.assertEquals(
                    EXPECTED_ROWS,
                    CloseFailingSink.receivedRows(),
                    "The failing close must not trigger a hidden redeploy of the pipeline");
            Assertions.assertTrue(
                    CloseFailingSink.closeFailures() >= 1,
                    "The sink close must actually have been invoked and must have failed");
            log.warn(
                    "Sink observed {} rows, {} close calls and {} close failures",
                    CloseFailingSink.receivedRows(),
                    CloseFailingSink.closeCalls(),
                    CloseFailingSink.closeFailures());

            // 3. FAILED must be the sticky terminal state after the coordinator's cleanup.
            Thread.sleep(3000L);
            Assertions.assertEquals(
                    JobStatus.FAILED,
                    jobProxy.getJobStatus(),
                    "The terminal FAILED state of a close-failed job must be stable after cleanup");

            // 3b. The engine's finished-job history must keep the FAILED state, the per-pipeline
            // terminal state and the close failure, so the outcome stays queryable after the
            // coordinator released the job master and its runtime state.
            NodeEngineImpl nodeEngine = node.node.nodeEngine;
            Address workerAddress = node.node.address;
            SeaTunnelServer seaTunnelServer = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            JobHistoryService jobHistoryService =
                    seaTunnelServer.getCoordinatorService().getJobHistoryService();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                JobHistoryService.JobState historyState =
                                        jobHistoryService.getJobDetailState(jobId);
                                Assertions.assertNotNull(
                                        historyState,
                                        "The finished-job history must keep the close-failed job");
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        historyState.getJobStatus(),
                                        "The history entry must report the close failure as FAILED");
                                Assertions.assertEquals(
                                        1,
                                        historyState.getPipelineStateMapperMap().size(),
                                        "The one-pipeline job must have one pipeline history entry");
                                Assertions.assertEquals(
                                        PipelineStatus.FAILED,
                                        historyState
                                                .getPipelineStateMapperMap()
                                                .values()
                                                .iterator()
                                                .next()
                                                .getPipelineStatus(),
                                        "The pipeline terminal state must be FAILED in history");
                                Assertions.assertNotNull(
                                        historyState.getErrorMessage(),
                                        "The history entry must keep the close failure message");
                                Assertions.assertTrue(
                                        historyState
                                                .getErrorMessage()
                                                .contains(CloseFailingSink.CLOSE_FAILURE_MESSAGE),
                                        "The history error must name the sink close failure, but "
                                                + "was: "
                                                + historyState.getErrorMessage());
                            });
            Assertions.assertTrue(
                    jobHistoryService.getJobStatusData().stream()
                            .anyMatch(
                                    state ->
                                            state.getJobId() == jobId
                                                    && state.getJobStatus() == JobStatus.FAILED),
                    "The finished-job listing must expose the close-failed job as FAILED");

            // 4. Every fixed slot must return to the pool.
            ResourceManager resourceManager =
                    seaTunnelServer.getCoordinatorService().getResourceManager();
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            0,
                                            resourceManager
                                                    .getRegisterWorker()
                                                    .get(workerAddress)
                                                    .getAssignedSlots()
                                                    .length,
                                            "A close-failed job must release every fixed slot"));

            IMap<PipelineLocation, Object> ownedSlotProfiles =
                    node.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
            Assertions.assertTrue(
                    ownedSlotProfiles.keySet().stream()
                            .noneMatch(location -> location.getJobId() == jobId),
                    "The close-failed job must own no pipeline slots after its cleanup");

            // The release is proven functionally as well: a job with the same two-slot footprint
            // must run to completion on the same cluster.
            JobConfig probeJobConfig = new JobConfig();
            probeJobConfig.setName(testCaseName + "_capacity_probe");
            ClientJobProxy probeJobProxy =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(CAPACITY_PROBE_CONF),
                                    probeJobConfig,
                                    seaTunnelConfig)
                            .execute();
            CompletableFuture<JobStatus> probeCompletionFuture =
                    CompletableFuture.supplyAsync(probeJobProxy::waitForJobComplete);
            Assertions.assertEquals(
                    JobStatus.FINISHED,
                    probeCompletionFuture.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    "A replacement job with the failed job's footprint must still finish; a "
                            + "PENDING probe means the failed close leaked capacity");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }
}
