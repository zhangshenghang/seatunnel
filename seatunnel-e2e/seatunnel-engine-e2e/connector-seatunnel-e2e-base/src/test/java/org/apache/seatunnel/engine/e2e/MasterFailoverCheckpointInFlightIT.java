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
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.e2e.masterfailovercheckpoint.SnapshotHoldGate;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end regression test for checkpoint-ID monotonicity across a master failover that happens
 * while a checkpoint is in flight.
 *
 * <p>Zeta allocates a checkpoint id by incrementing the per-pipeline counter in the shared counter
 * state store, and that increment is persisted at trigger time. If the active master dies after the
 * barrier was dispatched but before every task acknowledged it, the interrupted checkpoint never
 * reaches checkpoint storage. The surviving master rebuilds the coordinator from the latest
 * <em>completed</em> checkpoint and, in {@code CheckpointManager}, rewinds the counter to {@code
 * completedCheckpointId + 1}. When a checkpoint was in flight, that value is lower than the counter
 * the dead master had already persisted, so the next checkpoint re-issues an id that was already
 * allocated and partially acknowledged before the failover.
 *
 * <p>Re-issuing a checkpoint id is unsafe because {@code TaskAcknowledgeOperation} carries no
 * trigger epoch: it is matched to the pending checkpoint by id only. A task that was still inside
 * {@code snapshotState} for the old barrier can acknowledge the re-used id after the failover, and
 * the new checkpoint then mixes task state from the pre-failover barrier with state from the
 * post-failover barrier, producing a torn checkpoint that can later be restored.
 *
 * <p>The test drives the real cluster path: two dedicated master-only members plus one worker, a
 * streaming {@code FakeSource -> test sink} pipeline, and a sink writer that parks the second
 * checkpoint's barrier inside {@code snapshotState}. The first checkpoint completes normally so the
 * pipeline has a real checkpoint sequence; the active master is then shut down while the second
 * checkpoint is provably in flight and acknowledged by nobody. After the failover the released
 * writer acknowledges the old barrier. The contract asserted here is that the checkpoint published
 * after the failover must carry an id strictly greater than the id of the checkpoint that was in
 * flight, and that no stale pre-failover checkpoint is ever published.
 */
@Slf4j
public class MasterFailoverCheckpointInFlightIT {

    private static final String CONF_FILE =
            "master-failover-checkpoint/stream_fake_to_snapshot_hold_sink.conf";

    private static final String HOLD_KEY = "master-failover-checkpoint-in-flight";

    /** The single pipeline of {@link #CONF_FILE}; parallelism is 1. */
    private static final int PIPELINE_ID = 1;

    private static final long FIRST_CHECKPOINT_TIMEOUT_SECONDS = 90;

    private static final long FAILOVER_TIMEOUT_SECONDS = 120;

    private static final long CHECKPOINT_AFTER_FAILOVER_TIMEOUT_SECONDS = 120;

    @Test
    public void testCheckpointIdDoesNotRegressAcrossMasterFailoverWithInFlightCheckpoint()
            throws Exception {
        String testCaseName =
                "testCheckpointIdDoesNotRegressAcrossMasterFailoverWithInFlightCheckpoint";
        String testClusterName = "MasterFailoverCheckpointInFlightIT_" + testCaseName;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        try {
            masterNode1 =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            engineConfig(testClusterName));
            masterNode2 =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            engineConfig(testClusterName));
            workerNode =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            engineConfig(testClusterName));

            HazelcastInstanceImpl finalWorker = workerNode;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalWorker.getCluster().getMembers().size()));

            final HazelcastInstanceImpl observationNode = masterNode1;

            Common.setDeployMode(DeployMode.CLIENT);
            String confPath = TestUtils.getResource(CONF_FILE);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobProxy clientJobProxy =
                    engineClient
                            .createExecutionContext(
                                    confPath, jobConfig, engineConfig(testClusterName))
                            .execute();
            long jobId = clientJobProxy.getJobId();

            // The sink must prove the pipeline had a completed checkpoint before the gate is
            // armed: otherwise the failover would have no completed checkpoint to restore from and
            // the counter rewind this test targets would not happen.
            Awaitility.await()
                    .atMost(FIRST_CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            maxStoredCheckpointId(observationNode, jobId) >= 1,
                                            "The pipeline must complete its first checkpoint"
                                                    + " before the gate is armed"));
            Assertions.assertEquals(
                    JobStatus.RUNNING, clientJobProxy.getJobStatus(), "Job must still be running");

            SnapshotHoldGate.arm(HOLD_KEY);

            // Wait until a second checkpoint barrier has reached the sink writer and is parked
            // there: the checkpoint is triggered, acknowledged by no task and cannot complete.
            Awaitility.await()
                    .atMost(FIRST_CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1,
                                            SnapshotHoldGate.holdingWriters(HOLD_KEY),
                                            "A sink writer must be parked inside snapshotState"));
            long heldCheckpointId = nextCheckpointId(observationNode, jobId) - 1;
            Assertions.assertTrue(
                    heldCheckpointId >= 2,
                    "The held checkpoint must be a later checkpoint, but was " + heldCheckpointId);
            Assertions.assertFalse(
                    storedCheckpointExists(observationNode, jobId, heldCheckpointId),
                    "The in-flight checkpoint must not be stored while its sink is parked");

            HazelcastInstanceImpl activeMaster = jobMasterNode(masterNode1, masterNode2, jobId);
            HazelcastInstanceImpl survivingMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;
            log.warn(
                    "Killing the active master {} while checkpoint {} is in flight",
                    activeMaster.node.address,
                    heldCheckpointId);
            if (activeMaster == masterNode1) {
                masterNode1.shutdown();
                masterNode1 = null;
            } else {
                masterNode2.shutdown();
                masterNode2 = null;
            }

            HazelcastInstanceImpl finalSurvivingMaster = survivingMaster;
            Awaitility.await()
                    .atMost(FAILOVER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        2,
                                        finalSurvivingMaster.getCluster().getMembers().size(),
                                        "The surviving master and the worker must form the"
                                                + " cluster");
                                Assertions.assertNotNull(
                                        getJobMaster(finalSurvivingMaster, jobId),
                                        "The surviving master must take over the job");
                            });

            // The stale barrier is still held, so the interrupted checkpoint must still be
            // absent. This is the "never publish a pre-failover checkpoint" half of the contract.
            Assertions.assertFalse(
                    storedCheckpointExists(survivingMaster, jobId, heldCheckpointId),
                    "A checkpoint that was in flight when the active master died must never be"
                            + " published");

            // Let the pre-failover writer acknowledge its old barrier now that the new master is
            // in charge, and let the pipeline proceed with the checkpoint it schedules next.
            SnapshotHoldGate.release(HOLD_KEY);

            Awaitility.await()
                    .atMost(CHECKPOINT_AFTER_FAILOVER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            maxStoredCheckpointId(survivingMaster, jobId)
                                                    >= heldCheckpointId,
                                            "The new master must publish a checkpoint after the"
                                                    + " failover"));

            long checkpointAfterFailover = maxStoredCheckpointId(survivingMaster, jobId);
            Assertions.assertTrue(
                    checkpointAfterFailover > heldCheckpointId,
                    String.format(
                            "The checkpoint id must not be re-issued across a master failover:"
                                    + " checkpoint %d was already allocated and partially"
                                    + " acknowledged before the failover, but the next stored"
                                    + " checkpoint is %d",
                            heldCheckpointId, checkpointAfterFailover));

            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    clientJobProxy.getJobStatus(),
                    "The job must keep running after the failover");

            clientJobProxy.cancelJob();
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    clientJobProxy.waitForJobComplete(),
                    "The job must cancel cleanly after the failed-over checkpoint");
        } finally {
            SnapshotHoldGate.clear(HOLD_KEY);
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    /**
     * Creates the engine configuration of one member. Fixed slots keep the job footprint explicit:
     * the single pipeline has one split enumerator and one chained source-and-sink task group, so a
     * four-slot worker has ample capacity both before and after the failover.
     */
    private static SeaTunnelConfig engineConfig(String testClusterName) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config.getEngineConfig().getHttpConfig().setEnabled(false);
        config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        config.getEngineConfig().getSlotServiceConfig().setSlotNum(4);
        return config;
    }

    /** Returns the next checkpoint id the pipeline counter will hand out. */
    private static long nextCheckpointId(HazelcastInstanceImpl node, long jobId) {
        CounterStateStore<String> counterStore = checkpointCounterStore(node);
        Long value =
                counterStore.get(
                        StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, PIPELINE_ID));
        Assertions.assertNotNull(value, "The pipeline checkpoint counter must exist");
        return value;
    }

    private static CounterStateStore<String> checkpointCounterStore(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getEngineContext().getStateStores().checkpointCounterStore();
    }

    /**
     * Returns the highest checkpoint id currently durable for the job, or -1 when there is none.
     */
    private static long maxStoredCheckpointId(HazelcastInstanceImpl node, long jobId) {
        long max = -1;
        for (PipelineState state : storedCheckpoints(node, jobId)) {
            max = Math.max(max, state.getCheckpointId());
        }
        return max;
    }

    private static boolean storedCheckpointExists(
            HazelcastInstanceImpl node, long jobId, long checkpointId) {
        for (PipelineState state : storedCheckpoints(node, jobId)) {
            if (state.getCheckpointId() == checkpointId) {
                return true;
            }
        }
        return false;
    }

    private static List<PipelineState> storedCheckpoints(HazelcastInstanceImpl node, long jobId) {
        try {
            SeaTunnelServer server =
                    node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            CheckpointStorage storage = server.getCheckpointService().getCheckpointStorage();
            return storage.getAllCheckpoints(String.valueOf(jobId));
        } catch (CheckpointStorageException e) {
            // Convert to an AssertionError so Awaitility retries a transient storage read instead
            // of aborting the poll on the first attempt.
            throw new AssertionError("Failed to read checkpoints of job " + jobId, e);
        }
    }

    /** Reads the job master from the active SeaTunnel server embedded in the given test node. */
    private static JobMaster getJobMaster(HazelcastInstanceImpl activeMaster, long jobId) {
        SeaTunnelServer server =
                activeMaster.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService().getJobMaster(jobId);
    }

    /**
     * Finds the node that currently owns the job master. The first started member is normally the
     * active master, but resolving it from the job master itself keeps the test independent of
     * election order.
     */
    private static HazelcastInstanceImpl jobMasterNode(
            HazelcastInstanceImpl node1, HazelcastInstanceImpl node2, long jobId) {
        HazelcastInstanceImpl[] candidates = {node1, node2};
        for (HazelcastInstanceImpl candidate : candidates) {
            if (getJobMaster(candidate, jobId) != null) {
                return candidate;
            }
        }
        throw new AssertionError("No master node owns the job " + jobId);
    }
}
