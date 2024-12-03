package org.apache.seatunnel.engine.server.utils;

import java.util.LinkedList;

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

    /**
     * 添加新的资源利用率数据
     */
    public void addUtilizationData(double cpuUtilization, double memoryUtilization) {
        // 验证输入数据的有效性
        if (cpuUtilization < 0 || cpuUtilization > 1 ||
                memoryUtilization < 0 || memoryUtilization > 1) {
            throw new IllegalArgumentException("Utilization values must be between 0 and 1");
        }

        if (utilizationHistory.size() >= MAX_TIME_WINDOW) {
            utilizationHistory.removeLast(); // 移除最老的记录
        }
        utilizationHistory.addFirst(new UtilizationData(cpuUtilization, memoryUtilization));
    }

    /**
     * 根据实际记录数生成对应的时间权重
     */
    private double[] generateTimeWeights() {
        int size = utilizationHistory.size();
        if (size == 0) return new double[0];

        // 确定实际使用的权重数量
        int weightCount = Math.min(size, TIME_WEIGHT_RATIOS.length);
        double[] weights = new double[size];
        double totalWeight = 0;

        // 按照配置的比例分配权重
        for (int i = 0; i < size; i++) {
            weights[i] = (i < weightCount) ? TIME_WEIGHT_RATIOS[i] : TIME_WEIGHT_RATIOS[weightCount - 1];
            totalWeight += weights[i];
        }

        // 归一化权重，使总和为1
        for (int i = 0; i < size; i++) {
            weights[i] /= totalWeight;
        }

        return weights;
    }

    /**
     * 计算调度优先级
     */
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

    /**
     * 计算单个时间点的资源可用性
     */
    private double calculateResourceAvailability(UtilizationData data) {
        double cpuAvailability = 1.0 - data.cpuUtilization;
        double memoryAvailability = 1.0 - data.memoryUtilization;

        return (cpuAvailability * CPU_WEIGHT + memoryAvailability * MEMORY_WEIGHT)
                / (CPU_WEIGHT + MEMORY_WEIGHT);
    }

    // 测试方法
    public static void main(String[] args) {
        LoadBalancer loadBalancer = new LoadBalancer();

        // 测试3次记录的情况
        System.out.println("Testing with 3 records:");
        loadBalancer.addUtilizationData(0.6, 0.5); // 最老
        loadBalancer.addUtilizationData(0.7, 0.6);
        loadBalancer.addUtilizationData(0.5, 0.4); // 最新
        System.out.printf("Priority (3 records): %.4f%n", loadBalancer.calculateSchedulingPriority());

        // 测试不同数量记录的权重分布
        double[] weights = loadBalancer.generateTimeWeights();
        System.out.println("\nTime weights for 3 records:");
        for (int i = 0; i < weights.length; i++) {
            System.out.printf("Weight %d: %.4f%n", i + 1, weights[i]);
        }
    }
}