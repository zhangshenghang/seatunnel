package org.apache.seatunnel.engine.server.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;


public class LoadBalancerTest {

    private LoadBalancer loadBalancer;

    @BeforeEach
    void setUp() {
        loadBalancer = new LoadBalancer();
    }

    @Test
    @DisplayName("A newly created LoadBalancer should return the highest priority of 1.0")
    void newLoadBalancerShouldReturnMaxPriority() {
        System.out.println(loadBalancer.calculateSchedulingPriority());
        Assertions.assertEquals(1.0, loadBalancer.calculateSchedulingPriority());
    }

    @Test
    @DisplayName("Adding invalid utilization data should throw an exception")
    void shouldThrowExceptionForInvalidUtilizationData() {
        Assertions.assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> loadBalancer.addUtilizationData(-0.1, 0.5)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> loadBalancer.addUtilizationData(0.5, 1.1)),
                () -> assertThrows(IllegalArgumentException.class,
                        () -> loadBalancer.addUtilizationData(1.1, 0.5))
        );
    }

    @Test
    @DisplayName("Test weight calculation for 3 records")
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
                ((((1.0-0.6)*0.5) + ((1.0-0.5)*0.5))  * (4.0/8.0))+
                        // Second record (1-0.7)*0.5 + (1-0.6)*0.5  * (2/8)
                        ((((1.0-0.7)*0.5) + ((1.0-0.6)*0.5))  * (2.0/8.0)) +
                        // Oldest record (1-0.5)*0.5 + (1-0.4)*0.5 * (2/8)
                        ((((1.0-0.5)*0.5) + ((1.0-0.4)*0.5)) * (2.0/8.0));

        Assertions.assertEquals(expectedPriority, priority);
    }

    @Test
    @DisplayName("Test weight calculation for 5 records")
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
                ((((1.0-0.6)*0.5) + ((1.0-0.5)*0.5)) * (4.0/10.0)) +
                        // Second record: (1-0.7)*0.5 + (1-0.6)*0.5 * (2/10)
                        ((((1.0-0.7)*0.5) + ((1.0-0.6)*0.5)) * (2.0/10.0)) +
                        // Third record: (1-0.5)*0.5 + (1-0.4)*0.5 * (2/10)
                        ((((1.0-0.5)*0.5) + ((1.0-0.4)*0.5)) * (2.0/10.0)) +
                        // Fourth record: (1-0.4)*0.5 + (1-0.3)*0.5 * (1/10)
                        ((((1.0-0.4)*0.5) + ((1.0-0.3)*0.5)) * (1.0/10.0)) +
                        // Oldest record: (1-0.3)*0.5 + (1-0.2)*0.5 * (1/10)
                        ((((1.0-0.3)*0.5) + ((1.0-0.2)*0.5)) * (1.0/10.0));

        Assertions.assertEquals(expectedPriority, priority);
    }

    @Test
    @DisplayName("Detailed verification of adding 6 records (verifying the maximum window limit of 5)")
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
                ((((1.0 - 0.6) * 0.5) + ((1.0 - 0.5) * 0.5)) * (4.0 / 10.0)) +
                        // Second record: (1-0.7)*0.5 + (1-0.6)*0.5 * (2/10)
                        ((((1.0 - 0.7) * 0.5) + ((1.0 - 0.6) * 0.5)) * (2.0 / 10.0)) +
                        // Third record: (1-0.5)*0.5 + (1-0.4)*0.5 * (2/10)
                        ((((1.0 - 0.5) * 0.5) + ((1.0 - 0.4) * 0.5)) * (2.0 / 10.0)) +
                        // Fourth record: (1-0.4)*0.5 + (1-0.3)*0.5 * (1/10)
                        ((((1.0 - 0.4) * 0.5) + ((1.0 - 0.3) * 0.5)) * (1.0 / 10.0)) +
                        // Oldest record: (1-0.3)*0.5 + (1-0.2)*0.5 * (1/10)
                        ((((1.0 - 0.3) * 0.5) + ((1.0 - 0.2) * 0.5)) * (1.0 / 10.0));

        double actualPriority = loadBalancer.calculateSchedulingPriority();

        Assertions.assertEquals(expectedPriority, actualPriority);
    }
}