package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.server.resourcemanager.AbstractResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceRequestHandler;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoad;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.apache.commons.lang3.tuple.ImmutableTriple;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

public class LoadBalancerTest {

    private LoadBalancer loadBalancer;

    @BeforeEach
    void setUp() {
        loadBalancer = new LoadBalancer();
    }

    @Test
    @DisplayName("Step0: A newly created LoadBalancer should return the highest priority of 1.0")
    void newLoadBalancerShouldReturnMaxPriority() {
        System.out.println(loadBalancer.calculateSchedulingPriority());
        Assertions.assertEquals(1.0, loadBalancer.calculateSchedulingPriority());
    }

    @Test
    @DisplayName("Step1-3: Adding invalid utilization data should throw an exception")
    void shouldThrowExceptionForInvalidUtilizationData() {
        Assertions.assertAll(
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> loadBalancer.addUtilizationData(-0.1, 0.5)),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> loadBalancer.addUtilizationData(0.5, 1.1)),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> loadBalancer.addUtilizationData(1.1, 0.5)));
    }

    @Test
    @DisplayName("Step1-3: Test weight calculation for 3 records")
    void shouldCalculateCorrectPriorityForThreeRecords() {
        // Add 3 records
        // Oldest record
        loadBalancer.addUtilizationData(0.5, 0.4); // CPU: 50%, Memory: 40%
        loadBalancer.addUtilizationData(0.7, 0.6); // CPU: 70%, Memory: 60%
        // Newest record
        loadBalancer.addUtilizationData(0.6, 0.5); // CPU: 60%, Memory: 50%

        double priority = loadBalancer.calculateSchedulingPriority();

        // Manually calculate the expected result
        // Weight distribution should be [4/8, 2/8, 2/8]
        double expectedPriority =
                // Newest record (1-0.6)*0.5 + (1-0.5)*0.5  * (4/8)
                ((((1.0 - 0.6) * 0.5) + ((1.0 - 0.5) * 0.5)) * (4.0 / 8.0))
                        +
                        // Second record (1-0.7)*0.5 + (1-0.6)*0.5  * (2/8)
                        ((((1.0 - 0.7) * 0.5) + ((1.0 - 0.6) * 0.5)) * (2.0 / 8.0))
                        +
                        // Oldest record (1-0.5)*0.5 + (1-0.4)*0.5 * (2/8)
                        ((((1.0 - 0.5) * 0.5) + ((1.0 - 0.4) * 0.5)) * (2.0 / 8.0));

        Assertions.assertEquals(expectedPriority, priority);
    }

    @Test
    @DisplayName("Step1-3: Test weight calculation for 5 records")
    void shouldCalculateCorrectPriorityForFiveRecords() {
        // Add 5 records, from oldest to newest
        loadBalancer.addUtilizationData(0.3, 0.2);
        loadBalancer.addUtilizationData(0.4, 0.3);
        loadBalancer.addUtilizationData(0.5, 0.4);
        loadBalancer.addUtilizationData(0.7, 0.6);
        loadBalancer.addUtilizationData(0.6, 0.5);

        double priority = loadBalancer.calculateSchedulingPriority();

        // Manually calculate the expected result
        // Weight distribution should be [4/10, 2/10, 2/10, 1/10, 1/10]
        double expectedPriority =
                // Newest record: (1-0.6)*0.5 + (1-0.5)*0.5 * (4/10)
                ((((1.0 - 0.6) * 0.5) + ((1.0 - 0.5) * 0.5)) * (4.0 / 10.0))
                        +
                        // Second record: (1-0.7)*0.5 + (1-0.6)*0.5 * (2/10)
                        ((((1.0 - 0.7) * 0.5) + ((1.0 - 0.6) * 0.5)) * (2.0 / 10.0))
                        +
                        // Third record: (1-0.5)*0.5 + (1-0.4)*0.5 * (2/10)
                        ((((1.0 - 0.5) * 0.5) + ((1.0 - 0.4) * 0.5)) * (2.0 / 10.0))
                        +
                        // Fourth record: (1-0.4)*0.5 + (1-0.3)*0.5 * (1/10)
                        ((((1.0 - 0.4) * 0.5) + ((1.0 - 0.3) * 0.5)) * (1.0 / 10.0))
                        +
                        // Oldest record: (1-0.3)*0.5 + (1-0.2)*0.5 * (1/10)
                        ((((1.0 - 0.3) * 0.5) + ((1.0 - 0.2) * 0.5)) * (1.0 / 10.0));

        Assertions.assertEquals(expectedPriority, priority);
    }

    @Test
    @DisplayName(
            "Step1-3: Detailed verification of adding 6 records (verifying the maximum window limit of 5)")
    void detailedCalculationForSixRecords() {
        LoadBalancer loadBalancer = new LoadBalancer();

        // Add 6 records in chronological order (from oldest to newest)
        // The first record will be discarded because it exceeds the window limit of 5
        loadBalancer.addUtilizationData(0.2, 0.1); // Oldest record (will be discarded)
        loadBalancer.addUtilizationData(0.3, 0.2); // Now the oldest record
        loadBalancer.addUtilizationData(0.4, 0.3); // Fourth record
        loadBalancer.addUtilizationData(0.5, 0.4); // Third record
        loadBalancer.addUtilizationData(0.7, 0.6); // Second record
        loadBalancer.addUtilizationData(0.6, 0.5); // Newest record

        double expectedPriority =
                // Newest record: (1-0.6)*0.5 + (1-0.5)*0.5 * (4/10)
                ((((1.0 - 0.6) * 0.5) + ((1.0 - 0.5) * 0.5)) * (4.0 / 10.0))
                        +
                        // Second record: (1-0.7)*0.5 + (1-0.6)*0.5 * (2/10)
                        ((((1.0 - 0.7) * 0.5) + ((1.0 - 0.6) * 0.5)) * (2.0 / 10.0))
                        +
                        // Third record: (1-0.5)*0.5 + (1-0.4)*0.5 * (2/10)
                        ((((1.0 - 0.5) * 0.5) + ((1.0 - 0.4) * 0.5)) * (2.0 / 10.0))
                        +
                        // Fourth record: (1-0.4)*0.5 + (1-0.3)*0.5 * (1/10)
                        ((((1.0 - 0.4) * 0.5) + ((1.0 - 0.3) * 0.5)) * (1.0 / 10.0))
                        +
                        // Oldest record: (1-0.3)*0.5 + (1-0.2)*0.5 * (1/10)
                        ((((1.0 - 0.3) * 0.5) + ((1.0 - 0.2) * 0.5)) * (1.0 / 10.0));

        double actualPriority = loadBalancer.calculateSchedulingPriority();

        Assertions.assertEquals(expectedPriority, actualPriority);
    }

    @Test
    @DisplayName("Step4: Test calculateComprehensiveResourceAvailability method")
    void testCalculateComprehensiveResourceAvailability() throws UnknownHostException {
        // 假设综合资源空闲率为 0.8，Worker 节点已经连续分配了3个 Slot，这个值是通过实际内存和 CPU 计算得到
        double comprehensiveResourceAvailability = 0.8;

        LoadBalancer loadBalancer = new LoadBalancer();
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        when(workerProfile.getAddress()).thenReturn(new Address("127.0.0.1", 5701));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots =
                new ConcurrentHashMap<>();

        // 每次任务是固定的 Slot 资源
        double singleSlotResource =
                Math.round(((1 - comprehensiveResourceAvailability) / 5) * 100.0) / 100.0;
        int times = 0;

        // worker 还未分配时，综合资源空闲率不变
        double result =
                loadBalancer.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.8, result, 0.01);

        // worker 已经分配过1个 Slot
        times = 1;
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        result =
                loadBalancer.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.76, result, 0.01);

        // worker 已经分配过2个 Slot
        times = 2;
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        result =
                loadBalancer.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.72, result, 0.01);

        // 如果没有未分配 slot 不会执行

    }

    @Test
    @DisplayName("Step5: Test balanceFactor method")
    void testBalanceFactor() {
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[3]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[7]);
        double balanceFactor = loadBalancer.balanceFactor(workerProfile, 3);
        Assertions.assertEquals(0.7, balanceFactor, 0.01);
    }

    @Test
    @DisplayName("All: Test the overall calculation logic")
    void testLoadBalancer() throws UnknownHostException {

        // 验证方案一 : 拆分每一个步骤，验证每一个环节结算的指标是否准确
        LoadBalancer loadBalancer = new LoadBalancer();

        // Add 6 records in chronological order (from oldest to newest)
        // The first record will be discarded because it exceeds the window limit of 5
        loadBalancer.addUtilizationData(0.2, 0.1); // Oldest record (will be discarded)
        loadBalancer.addUtilizationData(0.3, 0.2); // Now the oldest record
        loadBalancer.addUtilizationData(0.4, 0.3); // Fourth record
        loadBalancer.addUtilizationData(0.5, 0.4); // Third record
        loadBalancer.addUtilizationData(0.7, 0.6); // Second record
        loadBalancer.addUtilizationData(0.6, 0.5); // Newest record
        double comprehensiveResourceAvailability = loadBalancer.calculateSchedulingPriority();
        Address address = new Address("127.0.0.1", 5701);
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        when(workerProfile.getAddress()).thenReturn(address);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots =
                new ConcurrentHashMap<>();

        // 每次任务是固定的 Slot 资源
        double singleSlotResource =
                Math.round(((1 - comprehensiveResourceAvailability) / 5) * 100.0) / 100.0;
        int times = 0;

        // worker 还未分配时，综合资源空闲率不变
        double result =
                loadBalancer.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.5, result, 0.01);

        // worker 已经分配过1个 Slot
        times = 1;
        workerAssignedSlots.put(address, new ImmutableTriple<>(singleSlotResource, 1, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        result =
                loadBalancer.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.4, result, 0.01);

        workerAssignedSlots.put(address, new ImmutableTriple<>(singleSlotResource, 2, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        result =
                loadBalancer.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double balanceFactor = loadBalancer.balanceFactor(workerProfile, 7);
        Assertions.assertEquals(0.12, balanceFactor, 0.01);

        double finalResult = 0.7 * 0.3 + 0.125 * 0.3;
        Assertions.assertEquals(
                finalResult,
                loadBalancer.calculateResourceAvailability(result, balanceFactor),
                0.01);

        // 验证方案二 : 模拟实际场景调用calculateWeight方法验证最终结果，验证和 step1 结果是否一致
        Map<Address, SystemLoad> workerLoadMap = new ConcurrentHashMap<>();
        SystemLoad systemLoad = new SystemLoad();
        LinkedHashMap<String, SystemLoad.SystemLoadInfo> metrics = new LinkedHashMap<>();
        metrics.put("20200101000001", new SystemLoad.SystemLoadInfo(0.3, 0.2));
        metrics.put("20200101000002", new SystemLoad.SystemLoadInfo(0.4, 0.3));
        metrics.put("20200101000003", new SystemLoad.SystemLoadInfo(0.5, 0.4));
        metrics.put("20200101000004", new SystemLoad.SystemLoadInfo(0.7, 0.6));
        metrics.put("20200101000005", new SystemLoad.SystemLoadInfo(0.6, 0.5));
        systemLoad.setMetrics(metrics);
        workerLoadMap.put(address, systemLoad);

        // 模拟当前节点资源
        WorkerProfile workerProfile2 = Mockito.mock(WorkerProfile.class);
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        when(workerProfile2.getAddress()).thenReturn(address);

        Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots2 =
                new ConcurrentHashMap<>();
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        // 模拟 ResourceManager
        AbstractResourceManager rm = Mockito.mock(AbstractResourceManager.class);
        when(rm.getEngineConfig()).thenReturn(Mockito.mock(EngineConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig())
                .thenReturn(Mockito.mock(SlotServiceConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig().getAllocateStrategy())
                .thenReturn(ServerConfigOptions.SLOT_ALLOCATE_STRATEGY.defaultValue());
        // 模拟 ResourceRequestHandler 调用 calculateWeight 计算权重
        ResourceRequestHandler resourceRequestHandler =
                new ResourceRequestHandler(1L, resourceProfiles, null, rm, workerLoadMap);
        resourceRequestHandler.calculateWeight(workerProfile2, workerAssignedSlots2);
        // 模拟申请资源
        workerAssignedSlots2.put(address, new ImmutableTriple<>(singleSlotResource, 1, 5));
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        resourceRequestHandler.calculateWeight(workerProfile2, workerAssignedSlots2);

        workerAssignedSlots2.put(address, new ImmutableTriple<>(singleSlotResource, 2, 5));
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        Assertions.assertEquals(
                resourceRequestHandler.calculateWeight(workerProfile2, workerAssignedSlots2),
                finalResult);
    }
}
