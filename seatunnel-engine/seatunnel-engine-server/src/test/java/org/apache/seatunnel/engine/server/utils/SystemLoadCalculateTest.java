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

public class SystemLoadCalculateTest {

    private SystemLoadCalculate systemLoadCalculate;

    @BeforeEach
    void setUp() {
        systemLoadCalculate = new SystemLoadCalculate();
    }

    @Test
    @DisplayName("Step0: A newly created LoadBalancer should return the highest priority of 1.0")
    void newLoadBalancerShouldReturnMaxPriority() {
        System.out.println(systemLoadCalculate.calculateSchedulingPriority());
        Assertions.assertEquals(1.0, systemLoadCalculate.calculateSchedulingPriority());
    }

    @Test
    @DisplayName("Step1-3: Adding invalid utilization data should throw an exception")
    void shouldThrowExceptionForInvalidUtilizationData() {
        Assertions.assertAll(
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> systemLoadCalculate.addUtilizationData(-0.1, 0.5)),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> systemLoadCalculate.addUtilizationData(0.5, 1.1)),
                () ->
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> systemLoadCalculate.addUtilizationData(1.1, 0.5)));
    }

    @Test
    @DisplayName("Step1-3: Test weight calculation for 3 records")
    void shouldCalculateCorrectPriorityForThreeRecords() {
        // Add 3 records
        // Oldest record
        systemLoadCalculate.addUtilizationData(0.5, 0.4); // CPU: 50%, Memory: 40%
        systemLoadCalculate.addUtilizationData(0.7, 0.6); // CPU: 70%, Memory: 60%
        // Newest record
        systemLoadCalculate.addUtilizationData(0.6, 0.5); // CPU: 60%, Memory: 50%

        double priority = systemLoadCalculate.calculateSchedulingPriority();

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
        systemLoadCalculate.addUtilizationData(0.3, 0.2);
        systemLoadCalculate.addUtilizationData(0.4, 0.3);
        systemLoadCalculate.addUtilizationData(0.5, 0.4);
        systemLoadCalculate.addUtilizationData(0.7, 0.6);
        systemLoadCalculate.addUtilizationData(0.6, 0.5);

        double priority = systemLoadCalculate.calculateSchedulingPriority();

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
        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();

        // Add 6 records in chronological order (from oldest to newest)
        // The first record will be discarded because it exceeds the window limit of 5
        systemLoadCalculate.addUtilizationData(0.2, 0.1); // Oldest record (will be discarded)
        systemLoadCalculate.addUtilizationData(0.3, 0.2); // Now the oldest record
        systemLoadCalculate.addUtilizationData(0.4, 0.3); // Fourth record
        systemLoadCalculate.addUtilizationData(0.5, 0.4); // Third record
        systemLoadCalculate.addUtilizationData(0.7, 0.6); // Second record
        systemLoadCalculate.addUtilizationData(0.6, 0.5); // Newest record

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

        double actualPriority = systemLoadCalculate.calculateSchedulingPriority();

        Assertions.assertEquals(expectedPriority, actualPriority);
    }

    @Test
    @DisplayName("Step4: Test calculateComprehensiveResourceAvailability method")
    void testCalculateComprehensiveResourceAvailability() throws UnknownHostException {
        // Assume that the overall resource idle rate is 0.8, and the Worker node has been
        // continuously allocated 3 slots. This value is calculated based on the actual memory and
        // CPU.
        double comprehensiveResourceAvailability = 0.8;

        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        Address address = new Address("127.0.0.1", 5701);
        when(workerProfile.getAddress()).thenReturn(address);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots =
                new ConcurrentHashMap<>();

        // Each task has a fixed slot resource
        double singleSlotResource =
                Math.round(((1 - comprehensiveResourceAvailability) / 5) * 100.0) / 100.0;
        int times = 0;

        // When the worker has not been assigned, the overall resource idle rate remains unchanged
        double result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.8, result, 0.01);

        // The worker has been assigned 1 slot
        times = 1;
        workerAssignedSlots.put(address, new ImmutableTriple<>(singleSlotResource, 1, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.76, result, 0.01);

        // The worker has been assigned 2 slots
        times = 2;
        workerAssignedSlots.put(address, new ImmutableTriple<>(singleSlotResource, 2, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.72, result, 0.01);

        // If there is no unassigned slot, it will not be executed.

    }

    @Test
    @DisplayName("Step5: Test balanceFactor method")
    void testBalanceFactor() {
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[3]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[7]);
        double balanceFactor = systemLoadCalculate.balanceFactor(workerProfile, 3);
        Assertions.assertEquals(0.7, balanceFactor, 0.01);
    }

    @Test
    @DisplayName("All: Test the overall calculation logic")
    void testLoadBalancer() throws UnknownHostException {

        // Verification plan 1: Split each step and verify whether the settlement indicators of each
        // link are accurate
        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();

        // Add 6 records in chronological order (from oldest to newest)
        // The first record will be discarded because it exceeds the window limit of 5
        systemLoadCalculate.addUtilizationData(0.2, 0.1); // Oldest record (will be discarded)
        systemLoadCalculate.addUtilizationData(0.3, 0.2); // Now the oldest record
        systemLoadCalculate.addUtilizationData(0.4, 0.3); // Fourth record
        systemLoadCalculate.addUtilizationData(0.5, 0.4); // Third record
        systemLoadCalculate.addUtilizationData(0.7, 0.6); // Second record
        systemLoadCalculate.addUtilizationData(0.6, 0.5); // Newest record
        double comprehensiveResourceAvailability =
                systemLoadCalculate.calculateSchedulingPriority();
        Address address = new Address("127.0.0.1", 5701);
        WorkerProfile workerProfile = Mockito.mock(WorkerProfile.class);
        when(workerProfile.getAddress()).thenReturn(address);
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots =
                new ConcurrentHashMap<>();

        // Each task has a fixed Slot resource
        double singleSlotResource =
                Math.round(((1 - comprehensiveResourceAvailability) / 5) * 100.0) / 100.0;
        int times = 0;

        // When the worker has not been assigned, the overall resource idle rate remains unchanged
        double result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double expected = comprehensiveResourceAvailability - (singleSlotResource * times);
        Assertions.assertEquals(expected, result, 0.01);
        Assertions.assertEquals(
                comprehensiveResourceAvailability - (singleSlotResource * times), result, 0.01);
        Assertions.assertEquals(0.5, result, 0.01);

        // The worker has been assigned 1 slot
        times = 1;
        workerAssignedSlots.put(address, new ImmutableTriple<>(singleSlotResource, 1, 0));
        when(workerProfile.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        result =
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
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
                systemLoadCalculate.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        double balanceFactor = systemLoadCalculate.balanceFactor(workerProfile, 7);
        Assertions.assertEquals(0.12, balanceFactor, 0.01);

        double finalResult = 0.7 * 0.3 + 0.125 * 0.3;
        Assertions.assertEquals(
                finalResult,
                systemLoadCalculate.calculateResourceAvailability(result, balanceFactor),
                0.01);

        // Verification plan 2: simulate the actual scenario and call the calculateWeight method to
        // verify the final result and whether it is consistent with the result of step 1
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

        // Mock current node resources
        WorkerProfile workerProfile2 = Mockito.mock(WorkerProfile.class);
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[5]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[3]);
        when(workerProfile2.getAddress()).thenReturn(address);

        Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots2 =
                new ConcurrentHashMap<>();
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        // Mock ResourceManager
        AbstractResourceManager rm = Mockito.mock(AbstractResourceManager.class);
        when(rm.getEngineConfig()).thenReturn(Mockito.mock(EngineConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig())
                .thenReturn(Mockito.mock(SlotServiceConfig.class));
        when(rm.getEngineConfig().getSlotServiceConfig().getAllocateStrategy())
                .thenReturn(ServerConfigOptions.SLOT_ALLOCATE_STRATEGY.defaultValue());
        // Simulate ResourceRequestHandler to call calculateWeight to calculate weight
        ResourceRequestHandler resourceRequestHandler =
                new ResourceRequestHandler(1L, resourceProfiles, null, rm, workerLoadMap);
        resourceRequestHandler.calculateWeight(workerProfile2, workerAssignedSlots2);
        // Mock Application Resources
        workerAssignedSlots2.put(address, new ImmutableTriple<>(singleSlotResource, 1, 5));
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[6]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[2]);
        resourceRequestHandler.calculateWeight(workerProfile2, workerAssignedSlots2);

        workerAssignedSlots2.put(address, new ImmutableTriple<>(singleSlotResource, 2, 5));
        when(workerProfile2.getAssignedSlots()).thenReturn(new SlotProfile[7]);
        when(workerProfile2.getUnassignedSlots()).thenReturn(new SlotProfile[1]);
        // Verity
        Assertions.assertEquals(
                resourceRequestHandler.calculateWeight(workerProfile2, workerAssignedSlots2),
                finalResult);
    }
}
