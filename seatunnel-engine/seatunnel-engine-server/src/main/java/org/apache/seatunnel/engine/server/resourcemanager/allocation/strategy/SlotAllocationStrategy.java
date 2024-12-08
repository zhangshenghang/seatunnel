package org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy;

import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.apache.commons.lang3.tuple.ImmutableTriple;

import com.hazelcast.cluster.Address;

import java.util.List;
import java.util.Map;
import java.util.Optional;

// Java
public interface SlotAllocationStrategy {
    Optional<WorkerProfile> selectWorker(
            List<WorkerProfile> availableWorkers,
            Map<Address, ImmutableTriple<Double, Integer, Integer>> workerAssignedSlots);
}
