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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.AllocateStrategy;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * Cluster fault tolerance test. Test the job recovery capability and data consistency assurance
 * capability in case of cluster node failure
 */
@Slf4j
public class AllocateStrategyIT {

    public static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    public static final String DYNAMIC_JOB_MODE = "dynamic_job_mode";

    public static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    public static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(TestUtils.getClusterName(testClusterName));
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        return seaTunnelConfig;
    }

    private static SeaTunnelConfig getSeaTunnelConfig2(String testClusterName) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig2());
        hazelcastConfig.setClusterName(TestUtils.getClusterName(testClusterName));
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        return seaTunnelConfig;
    }

    protected static String getHazelcastConfig2() {
        return "hazelcast:\n"
                + "  cluster-name: seatunnel\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        CLUSTER_WRITE:\n"
                + "          enabled: true\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - localhost\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: 5801\n"
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200";
    }

    protected static String getHazelcastConfig() {
        return "hazelcast:\n"
                + "  cluster-name: seatunnel\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        CLUSTER_WRITE:\n"
                + "          enabled: true\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - localhost\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: 5801\n"
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n"
                + "  member-attributes:\n"
                + "    group:\n"
                + "      type: string\n"
                + "      value: platform";
    }



    @Test
    public void testSlotRatioStrategy() throws Exception {
        String testCaseName = "testSlotRatioStrategy";
        String testClusterName = "TestSlotRatioStrategy";
        long testRowNumber = 100;
        int testParallelism = 4;

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        SlotServiceConfig slotServiceConfig = seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setSlotNum(10);
        slotServiceConfig.setDynamicSlot(false);
        slotServiceConfig.setAllocateStrategy(AllocateStrategy.SLOT_RATIO);

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(2, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(
                    createTestResources(testCaseName, JobMode.STREAMING, testRowNumber, testParallelism,"allocate_strategy_with_slot_ratio.conf"), jobConfig, seaTunnelConfig);
            jobExecutionEnv.execute();

            NodeEngineImpl nodeEngine = node1.node.nodeEngine;
            Address node2Address = node2.node.address;
            Address node1Address = node1.node.address;

            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();

            Awaitility.await().atMost(600, TimeUnit.SECONDS).untilAsserted(() -> {
                ConcurrentMap<Address, WorkerProfile> registerWorker = resourceManager.getRegisterWorker();
                int node1AssignedSlotsNum = registerWorker.get(node1Address).getAssignedSlots().length;
                int node2AssignedSlotsNum = registerWorker.get(node2Address).getAssignedSlots().length;
                Assertions.assertTrue(node1AssignedSlotsNum == 2 || node1AssignedSlotsNum == 3);
                Assertions.assertTrue(node2AssignedSlotsNum == 2 || node2AssignedSlotsNum == 3);
                Assertions.assertEquals(5, node1AssignedSlotsNum + node2AssignedSlotsNum);
            });

            // 启动一个占用 6 个节点的任务
            jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            SeaTunnelClient engineClient2 = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv2 = engineClient2.createExecutionContext(
                    createTestResources(testCaseName, JobMode.STREAMING, testRowNumber, 6,"allocate_strategy_with_slot_ratio.conf"), jobConfig, seaTunnelConfig);
            jobExecutionEnv2.execute();

            Awaitility.await().atMost(600, TimeUnit.SECONDS).untilAsserted(() -> {
                ConcurrentMap<Address, WorkerProfile> registerWorker = resourceManager.getRegisterWorker();
                int node1AssignedSlotsNum = registerWorker.get(node1Address).getAssignedSlots().length;
                int node2AssignedSlotsNum = registerWorker.get(node2Address).getAssignedSlots().length;
                Assertions.assertEquals(6, node1AssignedSlotsNum);
                Assertions.assertEquals(6, node2AssignedSlotsNum);
            });

            log.warn("========================clean test resource====================");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    public void testSystemLoadStrategy() throws Exception {
        String testCaseName = "testSystemLoadStrategy";
        String testClusterName = "TestSystemLoadStrategy";
        long testRowNumber = 100;
        int testParallelism = 4;

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        SlotServiceConfig slotServiceConfig = seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setSlotNum(10);
        slotServiceConfig.setDynamicSlot(false);
        slotServiceConfig.setAllocateStrategy(AllocateStrategy.SYSTEM_LOAD);
        MemberAttributeConfig node1Tags = new MemberAttributeConfig();
        node1Tags.setAttribute("strategy", "system_load1");
        seaTunnelConfig.getHazelcastConfig().setMemberAttributeConfig(node1Tags);

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            MemberAttributeConfig node2Tags = new MemberAttributeConfig();
            node2Tags.setAttribute("strategy", "system_load2");
            seaTunnelConfig.getHazelcastConfig().setMemberAttributeConfig(node2Tags);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(2, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv = engineClient.createExecutionContext(
                    createTestResources(testCaseName, JobMode.STREAMING, testRowNumber, testParallelism,"allocate_strategy_with_system_load.conf"), jobConfig, seaTunnelConfig);
            jobExecutionEnv.execute();

            engineClient.createExecutionContext(
                    createTestResources(testCaseName, JobMode.STREAMING, testRowNumber, testParallelism,"allocate_strategy_tag2_with_system_load.conf"), jobConfig, seaTunnelConfig).execute();

            NodeEngineImpl nodeEngine = node1.node.nodeEngine;
            Address node2Address = node2.node.address;
            Address node1Address = node1.node.address;

            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();

            Awaitility.await().atMost(600, TimeUnit.SECONDS).untilAsserted(() -> {
                ConcurrentMap<Address, WorkerProfile> registerWorker = resourceManager.getRegisterWorker();
                int node1AssignedSlotsNum = registerWorker.get(node1Address).getAssignedSlots().length;
                int node2AssignedSlotsNum = registerWorker.get(node2Address).getAssignedSlots().length;
                Assertions.assertTrue(node1AssignedSlotsNum == 5);
                Assertions.assertTrue(node2AssignedSlotsNum == 5);
                Assertions.assertEquals(10, node1AssignedSlotsNum + node2AssignedSlotsNum);
            });

            Thread.sleep(60000);
            // 启动一个占用 6 个节点的任务
            jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            SeaTunnelClient engineClient2 = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv2 = engineClient2.createExecutionContext(
                    createTestResources(testCaseName, JobMode.STREAMING, testRowNumber, 3,"allocate_strategy_no_tag_with_system_load.conf"), jobConfig, seaTunnelConfig);
            jobExecutionEnv2.execute();

            Awaitility.await().atMost(600, TimeUnit.SECONDS).untilAsserted(() -> {
                ConcurrentMap<Address, WorkerProfile> registerWorker = resourceManager.getRegisterWorker();
                int node1AssignedSlotsNum = registerWorker.get(node1Address).getAssignedSlots().length;
                int node2AssignedSlotsNum = registerWorker.get(node2Address).getAssignedSlots().length;
                Assertions.assertEquals(7, node1AssignedSlotsNum);
                Assertions.assertEquals(7, node2AssignedSlotsNum);
            });

            log.warn("========================clean test resource====================");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    /**
     * Create the test job config file basic on cluster_batch_fake_to_localfile_template.conf It
     * will delete the test sink target path before return the final job config file path
     *
     * @param testCaseName testCaseName
     * @param jobMode jobMode
     * @param rowNumber row.num per FakeSource parallelism
     * @param parallelism FakeSource parallelism
     */
    private String createTestResources(
            @NonNull String testCaseName, @NonNull JobMode jobMode, long rowNumber, int parallelism,String templateFile)
            throws IOException {
        checkArgument(rowNumber > 0, "rowNumber must greater than 0");
        checkArgument(parallelism > 0, "parallelism must greater than 0");
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);
        valueMap.put(DYNAMIC_JOB_MODE, jobMode.toString());
        valueMap.put(DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM, String.valueOf(rowNumber));
        valueMap.put(DYNAMIC_TEST_PARALLELISM, String.valueOf(parallelism));

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";

        TestUtils.createTestConfigFileFromTemplate(
                templateFile, valueMap, targetConfigFilePath);

        return targetConfigFilePath;
    }
}
