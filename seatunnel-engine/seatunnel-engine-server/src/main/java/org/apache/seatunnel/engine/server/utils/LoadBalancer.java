package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoad;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.apache.commons.lang3.tuple.ImmutableTriple;

import com.hazelcast.cluster.Address;

import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

public class LoadBalancer {
    // 配置部分
    private static final int MAX_TIME_WINDOW = 5; // 最大支持的记录数
    // 时间权重比例配置，从新到旧排列。可以配置任意长度，实际使用时会取min(当前记录数, 权重数组长度)
    private static final double[] TIME_WEIGHT_RATIOS = {4.0, 2.0, 2.0, 1.0, 1.0};
    // 资源权重配置
    private static final double CPU_WEIGHT = 0.5;
    private static final double MEMORY_WEIGHT = 0.5;

    private static class UtilizationData {
        private final double cpuUtilization;
        private final double memoryUtilization;

        public UtilizationData(double cpuUtilization, double memoryUtilization) {
            this.cpuUtilization = cpuUtilization;
            this.memoryUtilization = memoryUtilization;
        }
    }

    private final LinkedList<UtilizationData> utilizationHistory;

    public LoadBalancer() {
        this.utilizationHistory = new LinkedList<>();
    }

    /** 添加新的资源利用率数据 */
    public void addUtilizationData(double cpuUtilization, double memoryUtilization) {
        // 验证输入数据的有效性
        if (cpuUtilization < 0
                || cpuUtilization > 1
                || memoryUtilization < 0
                || memoryUtilization > 1) {
            throw new IllegalArgumentException("Utilization values must be between 0 and 1");
        }

        if (utilizationHistory.size() >= MAX_TIME_WINDOW) {
            utilizationHistory.removeLast(); // 移除最老的记录
        }
        utilizationHistory.addFirst(new UtilizationData(cpuUtilization, memoryUtilization));
    }

    /** 根据实际记录数生成对应的时间权重 */
    private double[] generateTimeWeights() {
        int size = utilizationHistory.size();
        if (size == 0) return new double[0];

        // 确定实际使用的权重数量
        int weightCount = Math.min(size, TIME_WEIGHT_RATIOS.length);
        double[] weights = new double[size];
        double totalWeight = 0;

        // 按照配置的比例分配权重
        for (int i = 0; i < size; i++) {
            weights[i] =
                    (i < weightCount) ? TIME_WEIGHT_RATIOS[i] : TIME_WEIGHT_RATIOS[weightCount - 1];
            totalWeight += weights[i];
        }

        // 归一化权重，使总和为1
        for (int i = 0; i < size; i++) {
            weights[i] /= totalWeight;
        }

        return weights;
    }

    /** 计算调度优先级 */
    public double calculateSchedulingPriority() {
        if (utilizationHistory.isEmpty()) {
            return 1.0; // 如果没有历史数据，返回最高优先级
        }

        double[] timeWeights = generateTimeWeights();
        double prioritySum = 0.0;
        int index = 0;

        for (UtilizationData data : utilizationHistory) {
            // 计算当前时间点的资源可用性
            double resourceAvailability = calculateResourceAvailability(data);
            // 应用时间权重
            prioritySum += resourceAvailability * timeWeights[index++];
        }

        return prioritySum;
    }

    public double calculate(
            SystemLoad systemLoads,
            WorkerProfile workerProfile,
            Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots) {
        if (Objects.isNull(systemLoads) || systemLoads.getMetrics().isEmpty()) {
            // If the node load is not obtained, zero is returned. This only happens when the
            // service is just started and the load status has not yet been obtained.
            return 0.0;
        }
        systemLoads
                .getMetrics()
                .forEach(
                        (k, v) -> {
                            Double cpuPercentage = v.getCpuPercentage();
                            Double memPercentage = v.getMemPercentage();
                            this.addUtilizationData(cpuPercentage, memPercentage);
                        });
        // 第三步计算出的综合资源空闲率
        double comprehensiveResourceAvailability = this.calculateSchedulingPriority();
        // 第四步结果
        double resourceAvailabilityStep4 =
                this.calculateComprehensiveResourceAvailability(
                        comprehensiveResourceAvailability, workerProfile, workerAssignedSlots);
        // 开始计算第五步，平衡因子
        double slotWeight = this.balanceFactor(workerProfile, workerAssignedSlots);
        double result = this.calculateResourceAvailability(resourceAvailabilityStep4, slotWeight);
        System.out.println(
                "node:"
                        + workerProfile.getAddress()
                        + "mem:"
                        + systemLoads.getMetrics()
                        + " 第三步："
                        + comprehensiveResourceAvailability
                        + " 第四步："
                        + resourceAvailabilityStep4
                        + " 第五步："
                        + slotWeight
                        + " 最终结果："
                        + result);
        return result;
    }

    public double calculateResourceAvailability(
            double resourceAvailabilityStep4, double slotWeight) {
        return 0.7 * resourceAvailabilityStep4 + 0.3 * slotWeight;
    }

    /** 计算单个时间点的资源可用性 */
    private double calculateResourceAvailability(UtilizationData data) {
        double cpuAvailability = 1.0 - data.cpuUtilization;
        double memoryAvailability = 1.0 - data.memoryUtilization;

        return (cpuAvailability * CPU_WEIGHT + memoryAvailability * MEMORY_WEIGHT)
                / (CPU_WEIGHT + MEMORY_WEIGHT);
    }

    /**
     * 第四步计算出的综合资源空闲率
     *
     * @return
     */
    public double calculateComprehensiveResourceAvailability(
            double comprehensiveResourceAvailability,
            WorkerProfile workerProfile,
            Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots) {
        // 开始第四步
        // 已经分配的 Slot 数量
        int assignedSlotsNum = workerProfile.getAssignedSlots().length;
        // 单个 Slot 使用的资源，默认 0.1
        double singleSlotUseResource = 0.1;
        ImmutableTriple<Double, Integer, Integer> tuple2 = null;
        if (workerAssignedSlots.get(workerProfile.getAddress()) == null) {
            if (assignedSlotsNum != 0) {
                singleSlotUseResource =
                        Math.round(
                                        ((1.0 - comprehensiveResourceAvailability)
                                                        / assignedSlotsNum)
                                                * 100.0)
                                / 100.0;
            }
            tuple2 =
                    workerAssignedSlots.getOrDefault(
                            workerProfile.getAddress(),
                            new ImmutableTriple<>(singleSlotUseResource, 0, assignedSlotsNum));
        } else {
            tuple2 = workerAssignedSlots.get(workerProfile.getAddress());
            singleSlotUseResource = tuple2.left;
        }

        Integer assignedTimesForTask = tuple2.middle;
        System.out.println(assignedTimesForTask);
        // 计算当前任务在 Worker 节点的权重，第四步计算完成
        comprehensiveResourceAvailability =
                comprehensiveResourceAvailability - (assignedTimesForTask * singleSlotUseResource);
        return comprehensiveResourceAvailability;
    }

    public double balanceFactor(
            WorkerProfile workerProfile,
            Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots) {
        ImmutableTriple<Double, Integer, Integer> tuple2 =
                workerAssignedSlots.get(workerProfile.getAddress());
        if (tuple2 != null) {
            System.out.println("已分配：" + tuple2.middle + tuple2.right);
            return balanceFactor(workerProfile, tuple2.middle + tuple2.right);
        } else {
            return balanceFactor(workerProfile, workerProfile.getAssignedSlots().length);
        }
    }

    public double balanceFactor(WorkerProfile workerProfile, Integer assignedSlots) {
        return 1.0
                - ((double) assignedSlots
                        / (workerProfile.getAssignedSlots().length
                                + workerProfile.getUnassignedSlots().length));
    }

    // 测试方法
    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer();

        // 测试3次记录的情况
        System.out.println("Testing with 3 records:");
        loadBalancer.addUtilizationData(0.6, 0.5); // 最老
        loadBalancer.addUtilizationData(0.7, 0.6);
        loadBalancer.addUtilizationData(0.5, 0.4); // 最新
        System.out.printf(
                "Priority (3 records): %.4f%n", loadBalancer.calculateSchedulingPriority());

        // 测试不同数量记录的权重分布
        double[] weights = loadBalancer.generateTimeWeights();
        System.out.println("\nTime weights for 3 records:");
        for (int i = 0; i < weights.length; i++) {
            System.out.printf("Weight %d: %.4f%n", i + 1, weights[i]);
        }
    }
}
