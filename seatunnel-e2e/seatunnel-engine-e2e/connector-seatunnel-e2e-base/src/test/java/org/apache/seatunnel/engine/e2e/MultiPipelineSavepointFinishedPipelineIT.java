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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.e2e.multipipeline.SavepointPipelineHoldGate;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * End-to-end coverage for stop-with-savepoint on a multi-pipeline job whose sibling pipeline has
 * already finished cleanly.
 *
 * <p><b>The contract.</b> A multi-pipeline job only ends when every pipeline has ended, so a job
 * can be {@code RUNNING} while one of its pipelines is already {@code FINISHED}. Triggering a
 * savepoint at that moment must savepoint the pipelines that are still alive and leave the finished
 * one alone:
 *
 * <ol>
 *   <li>the savepoint request succeeds instead of being rejected with {@code
 *       CHECKPOINT_COORDINATOR_SHUTDOWN} because a completed pipeline's coordinator is no longer
 *       accepting savepoints;
 *   <li>the running pipeline's savepoint is written to checkpoint storage and the job reaches
 *       {@code SAVEPOINT_DONE}, so the caller gets a usable recovery point;
 *   <li>the finished pipeline's final checkpoint is recorded durably as the completion proof of the
 *       pipeline the savepoint skips, and is not deleted by the savepoint;
 *   <li>the savepoint does not fall back to the failure path ({@code savepointFailed()} -> {@code
 *       stopJob()}), which would force-stop the healthy pipeline and report the savepoint as
 *       failed;
 *   <li>the restore of that savepoint resumes the pipeline that took it, does not deploy the
 *       pipeline that had already delivered its data, and leaves that pipeline's output untouched.
 * </ol>
 *
 * <p><b>Why the fixture holds the pipeline.</b> The test needs the asymmetric lifecycle to last
 * until its assertions are done. The fixture's second pipeline reads a test-only bounded source
 * whose reader emits every row and then waits for {@link SavepointPipelineHoldGate} before it
 * signals the end of its input, so the pipeline cannot finish on its own and a checkpoint or
 * savepoint barrier still reaches it while it is parked. The restore half arms a fresh hold, sees
 * the restored pipeline running, and only then releases it for its final completion. No assertion
 * depends on how long a finite read delay happens to last.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code MultiPipelineSavepointRestoreIT}
 * (the two-active-pipeline savepoint/restore test) and {@code SavepointRestoreIT} only exercise
 * jobs whose pipelines are all still running; {@code SavepointBusySourceBarrierIT} and {@code
 * SavepointPreconditionRecoveryIT} cover a single pipeline. No E2E submits a savepoint to a
 * multi-pipeline job with a cleanly finished sibling pipeline.
 */
public class MultiPipelineSavepointFinishedPipelineIT {

    private static final String JOB_TEMPLATE =
            "multi-pipeline-finished-savepoint/batch_two_pipeline_one_finished_template.conf";

    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    private static final String DYNAMIC_UNUSED_HOLD_KEY = "dynamic_unused_hold_key";

    private static final String DYNAMIC_OUTPUT_PATH = "dynamic_output_path";

    /** The fixture has two independent source pipelines. */
    private static final int PIPELINE_COUNT = 2;

    private static final int PIPELINE_ID_START = 1;

    /** Rows the pipeline that finishes first writes; keep in sync with the fixture. */
    private static final long FAST_PIPELINE_ROW_NUM = 10L;

    private static final long STATUS_TIMEOUT_SECONDS = 120;

    @Test
    public void testSavepointWhileSiblingPipelineAlreadyFinished() throws Exception {
        String testName =
                "MultiPipelineSavepointFinishedPipelineIT_testSavepointWhileSiblingFinished";
        String clusterName = TestUtils.getClusterName(testName);
        int clusterPort = allocateClusterPort(testName);
        String holdKey = testName + "-hold";
        String outputPath = createOutputDirectory(testName);
        String jobConfigPath = createJobConfig(testName, holdKey, testName + "-unused", outputPath);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;
        SavepointPipelineHoldGate.arm(holdKey);
        try {
            SeaTunnelConfig seaTunnelConfig = newSeaTunnelConfig(clusterName, clusterPort);
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            engineClient = new SeaTunnelClient(newClientConfig(clusterName, clusterPort));

            job = submit(engineClient, jobConfigPath, seaTunnelConfig);
            final long jobId = job.getJobId();
            final SeaTunnelClient client = engineClient;
            final ClientJobProxy submittedJob = job;
            final HazelcastInstanceImpl testNode = node;

            // The gate holds the second pipeline after it read every row, so the asymmetric state
            // is stable: one pipeline is FINISHED and the sibling cannot finish on its own.
            int finishedPipelineId = awaitOneFinishedOneRunning(testNode, jobId, holdKey);
            int runningPipelineId = otherPipeline(finishedPipelineId);

            JobMaster jobMaster = getJobMaster(testNode, jobId);
            Assertions.assertNotNull(jobMaster, "The running job must have a job master");
            CheckpointManager checkpointManager = jobMaster.getCheckpointManager();
            final CheckpointCoordinator finishedCoordinator =
                    checkpointManager.getCheckpointCoordinator(finishedPipelineId);
            // The pipeline status can turn FINISHED while the coordinator is still notifying the
            // final checkpoint, and such a pipeline is deliberately not skippable yet. Wait for the
            // terminal clean outcome the contract is about.
            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            finishedCoordinator.isCleanlyCompleted(),
                                            "The finished pipeline must have reached its clean"
                                                    + " terminal outcome before the savepoint"
                                                    + " request (isCompleted="
                                                    + finishedCoordinator.isCompleted()
                                                    + ", isEndOfSavePoint="
                                                    + finishedCoordinator.isEndOfSavePoint()
                                                    + ")"));
            Assertions.assertTrue(
                    finishedCoordinator.isCompleted(),
                    "The finished pipeline's checkpoint coordinator must be completed before the"
                            + " savepoint request");
            Assertions.assertFalse(
                    checkpointManager.getCheckpointCoordinator(runningPipelineId).isCompleted(),
                    "The running pipeline's checkpoint coordinator must still be active");
            Assertions.assertFalse(
                    checkpointManager.isSavePointEnd(),
                    "A job without any savepoint must not report savepoint completion");
            Assertions.assertEquals(
                    FAST_PIPELINE_ROW_NUM,
                    countWrittenRows(outputPath),
                    "The pipeline that finishes first must have written its rows exactly once");

            // Core regression assertion: the savepoint must succeed even though one pipeline of
            // the job already ended. Pre-fix this throws a SeaTunnelEngineException caused by
            // CheckpointException(CHECKPOINT_COORDINATOR_SHUTDOWN) from the finished pipeline.
            Assertions.assertDoesNotThrow(
                    () -> client.savePointJob(jobId),
                    "A savepoint request must not fail because a sibling pipeline already"
                            + " finished");

            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.SAVEPOINT_DONE,
                                            submittedJob.getJobStatus(),
                                            "The job must end SAVEPOINT_DONE, not through the"
                                                    + " savepoint-failure stop path"));

            Assertions.assertTrue(
                    checkpointManager
                            .getCheckpointCoordinator(runningPipelineId)
                            .isEndOfSavePoint(),
                    "The running pipeline must have written a savepoint checkpoint");
            Assertions.assertFalse(
                    checkpointManager
                            .getCheckpointCoordinator(finishedPipelineId)
                            .isEndOfSavePoint(),
                    "The finished pipeline must be left alone; its last checkpoint is a completed"
                            + " final checkpoint, not a savepoint");
            Assertions.assertTrue(
                    checkpointManager.isSavePointEnd(),
                    "The job-level savepoint-end check must accept a pipeline that finished"
                            + " before the savepoint; otherwise terminal cleanup may delete the"
                            + " savepoint data");
            // The savepoint phase is over: abort the hold so the reader of the suspended pipeline
            // stops waiting instead of keeping its task thread parked through the teardown.
            SavepointPipelineHoldGate.clear(holdKey);

            CheckpointStorage checkpointStorage = getCheckpointStorage(testNode);
            Assertions.assertTrue(
                    hasSavepointCheckpoint(checkpointStorage, jobId, runningPipelineId),
                    "The savepoint of the running pipeline must be persisted in checkpoint"
                            + " storage");
            // A restore only accepts the missing savepoint state of the finished pipeline when the
            // savepoint carries its durable completion proof: the completed-point checkpoint that
            // the regular finalization never writes.
            Assertions.assertEquals(
                    CheckpointType.COMPLETED_POINT_TYPE,
                    latestCheckpointType(checkpointStorage, jobId, finishedPipelineId),
                    "The savepoint must record the completion marker of the pipeline it skips, so a"
                            + " restore can tell it from lost state");
            Assertions.assertEquals(
                    FAST_PIPELINE_ROW_NUM,
                    countWrittenRows(outputPath),
                    "The savepoint must not run the finished pipeline again");

            // The savepoint path must release the job master instead of leaving the job in
            // DOING_SAVEPOINT with a retained coordinator.
            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .untilAsserted(
                            () ->
                                    Assertions.assertNull(
                                            getCoordinatorService(testNode).getJobMaster(jobId),
                                            "A savepointed job must release its job master"));
        } finally {
            SavepointPipelineHoldGate.clear(holdKey);
            cancelQuietly(job);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    /**
     * The restore half of the same contract: the savepoint only contains state for the pipeline
     * that was still running, so a restore must resume that pipeline and must not deploy the
     * sibling that had already finished cleanly before the savepoint.
     */
    @Test
    public void testRestoreAfterSavepointWithFinishedSiblingDoesNotReviveIt() throws Exception {
        String testName = "MultiPipelineSavepointFinishedPipelineIT_testRestoreAfterSavepoint";
        String clusterName = TestUtils.getClusterName(testName);
        int clusterPort = allocateClusterPort(testName);
        String holdKey = testName + "-hold";
        String outputPath = createOutputDirectory(testName);
        String jobConfigPath = createJobConfig(testName, holdKey, testName + "-unused", outputPath);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;
        ClientJobProxy restoredJob = null;
        SavepointPipelineHoldGate.arm(holdKey);
        try {
            SeaTunnelConfig seaTunnelConfig = newSeaTunnelConfig(clusterName, clusterPort);
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            final HazelcastInstanceImpl testNode = node;
            engineClient = new SeaTunnelClient(newClientConfig(clusterName, clusterPort));

            job = submit(engineClient, jobConfigPath, seaTunnelConfig);
            final long jobId = job.getJobId();
            final int finishedPipelineId = awaitOneFinishedOneRunning(node, jobId, holdKey);
            final int runningPipelineId = otherPipeline(finishedPipelineId);
            final CheckpointCoordinator finishedCoordinator =
                    getJobMaster(node, jobId)
                            .getCheckpointManager()
                            .getCheckpointCoordinator(finishedPipelineId);
            // The pipeline status can turn FINISHED while the coordinator is still notifying the
            // final checkpoint, and such a pipeline is deliberately not skippable yet.
            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            finishedCoordinator.isCleanlyCompleted(),
                                            "The finished pipeline must have reached its clean"
                                                    + " terminal outcome before the savepoint"
                                                    + " request"));
            long rowsWrittenBeforeSavepoint = countWrittenRows(outputPath);
            Assertions.assertEquals(
                    FAST_PIPELINE_ROW_NUM,
                    rowsWrittenBeforeSavepoint,
                    "The pipeline that finishes first must have written its rows exactly once");

            engineClient.savePointJob(jobId);
            final ClientJobProxy savepointedJob = job;
            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.SAVEPOINT_DONE,
                                            savepointedJob.getJobStatus(),
                                            "The job must be savepointed before it is restored"));

            // Abort the hold of the savepointed run, then arm a fresh one: the restored pipeline
            // must be observed running and only the explicit release below may let it finish.
            SavepointPipelineHoldGate.clear(holdKey);
            SavepointPipelineHoldGate.arm(holdKey);

            JobConfig restoreJobConfig = new JobConfig();
            restoreJobConfig.setName("multi_pipeline_finished_savepoint_restore");
            final ClientJobProxy restored =
                    engineClient
                            .restoreExecutionContext(
                                    jobConfigPath, restoreJobConfig, seaTunnelConfig, jobId)
                            .execute();
            restoredJob = restored;
            Assertions.assertEquals(
                    jobId, restored.getJobId(), "A savepoint restore must keep the source job id");

            // The savepointed pipeline must resume while the pipeline that ended before the
            // savepoint stays finished: deploying it again would re-run data that already reached
            // the sinks.
            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .ignoreExceptions()
                    .untilAsserted(
                            () -> {
                                Map<Integer, PipelineStatus> statuses =
                                        pipelineStatuses(testNode, jobId);
                                Assertions.assertEquals(
                                        PipelineStatus.FINISHED,
                                        statuses.get(finishedPipelineId),
                                        "The pipeline that finished before the savepoint must not"
                                                + " be deployed again by the restore: "
                                                + statuses);
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING,
                                        statuses.get(runningPipelineId),
                                        "The savepointed pipeline must resume: " + statuses);
                                Assertions.assertEquals(
                                        1,
                                        SavepointPipelineHoldGate.holdingReaders(holdKey),
                                        "The restored pipeline must be parked at the end of its"
                                                + " input, not finished: "
                                                + statuses);
                            });

            // Explicit release: only now may the restored pipeline finish.
            SavepointPipelineHoldGate.release(holdKey);

            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .ignoreExceptions()
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.FINISHED,
                                            restored.getJobStatus(),
                                            "The restored batch job must finish after the release"));
            Assertions.assertEquals(
                    PipelineStatus.FINISHED,
                    pipelineStatuses(testNode, jobId).get(finishedPipelineId),
                    "The already finished pipeline must stay finished through the restored run");
            Assertions.assertEquals(
                    rowsWrittenBeforeSavepoint,
                    countWrittenRows(outputPath),
                    "The restored job must not run the pipeline that already delivered its data"
                            + " again; that would replay its rows into the sinks");
        } finally {
            SavepointPipelineHoldGate.clear(holdKey);
            cancelQuietly(restoredJob);
            cancelQuietly(job);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    /**
     * Port range this test's in-JVM cluster uses.
     *
     * <p>Every test class of this module configures its own cluster name but the same seed port, so
     * two engine E2E suites running at the same time would fight over it. Deriving a stable port
     * range from the test name keeps the two test methods of this class, and this class against
     * other suites, on separate ports.
     */
    private static int allocateClusterPort(String testName) {
        return 20000 + Math.floorMod(testName.hashCode(), 5000) * 2;
    }

    private static SeaTunnelConfig newSeaTunnelConfig(String clusterName, int clusterPort) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        // Engine E2E suites can share a host when several of them run at the same time, and every
        // cluster would then contend for the default seed port. This test keeps its cluster and its
        // client on a port range of its own so a client can never authenticate against a foreign
        // cluster.
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPort(clusterPort);
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        seaTunnelConfig
                .getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setMembers(Collections.singletonList("localhost:" + clusterPort));
        // The REST endpoint is not used here and would otherwise contend for a fixed port.
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static ClientConfig newClientConfig(String clusterName, int clusterPort) {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig
                .getNetworkConfig()
                .setAddresses(Collections.singletonList("localhost:" + clusterPort));
        return clientConfig;
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient, String jobConfigPath, SeaTunnelConfig seaTunnelConfig)
            throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("multi_pipeline_finished_savepoint");
        return engineClient
                .createExecutionContext(jobConfigPath, jobConfig, seaTunnelConfig)
                .execute();
    }

    /** Creates the per-test sink output directory and returns its path. */
    private static String createOutputDirectory(String testName) throws IOException {
        String outputPath =
                System.getProperty("java.io.tmpdir")
                        + File.separator
                        + "seatunnel-multi-pipeline-savepoint"
                        + File.separator
                        + testName;
        Path directory = Paths.get(outputPath);
        if (Files.exists(directory)) {
            try (Stream<Path> files = Files.walk(directory)) {
                for (Path file :
                        files.sorted((left, right) -> right.compareTo(left))
                                .collect(Collectors.toList())) {
                    Files.deleteIfExists(file);
                }
            }
        }
        Files.createDirectories(directory);
        return outputPath;
    }

    /** Renders the fixture template with the keys and the output directory of this test. */
    private static String createJobConfig(
            String testName, String holdKey, String unusedHoldKey, String outputPath)
            throws IOException {
        Map<String, String> values = new HashMap<>();
        values.put(DYNAMIC_HOLD_KEY, holdKey);
        values.put(DYNAMIC_UNUSED_HOLD_KEY, unusedHoldKey);
        values.put(DYNAMIC_OUTPUT_PATH, outputPath);
        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(JOB_TEMPLATE, values, targetConfigFilePath);
        return targetConfigFilePath;
    }

    /**
     * Waits for the exact state under test and returns the pipeline id that already finished. The
     * gate count makes sure the sibling that is still {@code RUNNING} is the pipeline that the
     * fixture parks, so this state cannot disappear by the pipeline finishing on its own.
     */
    private static int awaitOneFinishedOneRunning(
            HazelcastInstanceImpl node, long jobId, String holdKey) {
        final int[] finished = {-1};
        Awaitility.await()
                .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            Map<Integer, PipelineStatus> statuses = pipelineStatuses(node, jobId);
                            Assertions.assertEquals(
                                    PIPELINE_COUNT,
                                    statuses.size(),
                                    "Expected one state entry per pipeline: " + statuses);
                            long finishedCount =
                                    statuses.values().stream()
                                            .filter(status -> status == PipelineStatus.FINISHED)
                                            .count();
                            long runningCount =
                                    statuses.values().stream()
                                            .filter(status -> status == PipelineStatus.RUNNING)
                                            .count();
                            Assertions.assertEquals(
                                    1,
                                    finishedCount,
                                    "Exactly one pipeline must have finished before the savepoint:"
                                            + " "
                                            + statuses);
                            Assertions.assertEquals(
                                    1,
                                    runningCount,
                                    "The sibling pipeline must still be running before the"
                                            + " savepoint: "
                                            + statuses);
                            Assertions.assertEquals(
                                    1,
                                    SavepointPipelineHoldGate.holdingReaders(holdKey),
                                    "The sibling pipeline must be parked at the end of the input"
                                            + " the fixture holds, otherwise it can finish before"
                                            + " the savepoint is asserted: "
                                            + statuses);
                            finished[0] =
                                    statuses.entrySet().stream()
                                            .filter(
                                                    entry ->
                                                            entry.getValue()
                                                                    == PipelineStatus.FINISHED)
                                            .map(Map.Entry::getKey)
                                            .findFirst()
                                            .orElse(-1);
                        });
        return finished[0];
    }

    private static int otherPipeline(int pipelineId) {
        for (int candidate = PIPELINE_ID_START;
                candidate < PIPELINE_ID_START + PIPELINE_COUNT;
                candidate++) {
            if (candidate != pipelineId) {
                return candidate;
            }
        }
        throw new IllegalStateException("No sibling pipeline for " + pipelineId);
    }

    private static Map<Integer, PipelineStatus> pipelineStatuses(
            HazelcastInstanceImpl node, long jobId) {
        Map<Integer, PipelineStatus> statuses = new HashMap<>();
        for (int pipelineId = PIPELINE_ID_START;
                pipelineId < PIPELINE_ID_START + PIPELINE_COUNT;
                pipelineId++) {
            Object state =
                    node.getMap(Constant.IMAP_RUNNING_JOB_STATE)
                            .get(new PipelineLocation(jobId, pipelineId));
            if (state instanceof PipelineStatus) {
                statuses.put(pipelineId, (PipelineStatus) state);
            }
        }
        return statuses;
    }

    /** Counts the rows the sink of the pipeline that finishes first wrote so far. */
    private static long countWrittenRows(String outputPath) throws IOException {
        Path directory = Paths.get(outputPath);
        if (!Files.exists(directory)) {
            return 0L;
        }
        long rows = 0L;
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
                try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                    rows += lines.count();
                }
            }
        }
        return rows;
    }

    private static boolean hasSavepointCheckpoint(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId) throws Exception {
        List<PipelineState> states =
                checkpointStorage.getCheckpointsByJobIdAndPipelineId(
                        String.valueOf(jobId), String.valueOf(pipelineId));
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        for (PipelineState state : states) {
            CompletedCheckpoint checkpoint =
                    serializer.deserialize(state.getStates(), CompletedCheckpoint.class);
            if (checkpoint.getCheckpointType().isSavepoint()) {
                return true;
            }
        }
        return false;
    }

    private static CheckpointType latestCheckpointType(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId) throws Exception {
        PipelineState state =
                checkpointStorage.getLatestCheckpointByJobIdAndPipelineId(
                        String.valueOf(jobId), String.valueOf(pipelineId));
        Assertions.assertNotNull(
                state,
                "A checkpoint record must exist for the pipeline that finished before the"
                        + " savepoint, otherwise the savepoint has no durable completion proof");
        return new ProtoStuffSerializer()
                .deserialize(state.getStates(), CompletedCheckpoint.class)
                .getCheckpointType();
    }

    private static CheckpointStorage getCheckpointStorage(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCheckpointService().getCheckpointStorage();
    }

    private static JobMaster getJobMaster(HazelcastInstanceImpl node, long jobId) {
        return getCoordinatorService(node).getJobMaster(jobId);
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
    }

    private static void cancelQuietly(ClientJobProxy job) {
        if (job == null) {
            return;
        }
        try {
            JobStatus status = job.getJobStatus();
            if (status != null && !status.isEndState()) {
                job.cancelJob();
            }
        } catch (Exception ignored) {
            // best-effort cleanup: the cluster is shut down right after this call
        }
    }
}
