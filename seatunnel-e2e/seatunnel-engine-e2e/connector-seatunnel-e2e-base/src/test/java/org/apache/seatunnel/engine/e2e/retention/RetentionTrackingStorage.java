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

package org.apache.seatunnel.engine.e2e.retention;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.localfile.LocalFileStorage;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real local-file checkpoint storage that records every successful write and every delete request.
 *
 * <p>The coordinator's retention policy is expressed as storage side effects: when it rotates, it
 * asks storage to delete a block of checkpoint ids. This fixture keeps the {@code (pipeline,
 * checkpoint id)} pairs the engine actually persisted and the delete requests it issued, so the E2E
 * can prove that retention only ever deletes checkpoints that the same pipeline had already stored
 * successfully.
 */
public class RetentionTrackingStorage extends LocalFileStorage {

    private static final Map<String, List<Long>> STORED_IDS = new ConcurrentHashMap<>();

    private static final List<DeleteRequest> DELETE_REQUESTS = new CopyOnWriteArrayList<>();

    private static final AtomicInteger JOB_LEVEL_DELETES = new AtomicInteger();

    public RetentionTrackingStorage(Map<String, String> configuration) {
        super(configuration);
    }

    /** Clears the static bookkeeping before a test starts. */
    public static void reset() {
        STORED_IDS.clear();
        DELETE_REQUESTS.clear();
        JOB_LEVEL_DELETES.set(0);
    }

    public static List<Long> storedIds(String jobId, int pipelineId) {
        return STORED_IDS.getOrDefault(key(jobId, pipelineId), Collections.emptyList());
    }

    public static List<DeleteRequest> deleteRequests() {
        return DELETE_REQUESTS;
    }

    public static List<DeleteRequest> deleteRequestsOf(int pipelineId) {
        List<DeleteRequest> requests = new CopyOnWriteArrayList<>();
        for (DeleteRequest request : DELETE_REQUESTS) {
            if (request.getPipelineId() == pipelineId) {
                requests.add(request);
            }
        }
        return requests;
    }

    public static int jobLevelDeletes() {
        return JOB_LEVEL_DELETES.get();
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        String location = super.storeCheckPoint(state);
        STORED_IDS
                .computeIfAbsent(
                        key(state.getJobId(), state.getPipelineId()),
                        ignored -> new CopyOnWriteArrayList<>())
                .add(state.getCheckpointId());
        return location;
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        DELETE_REQUESTS.add(
                new DeleteRequest(
                        Integer.parseInt(pipelineId),
                        Collections.singletonList(Long.parseLong(checkpointId))));
        super.deleteCheckpoint(jobId, pipelineId, checkpointId);
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIdList)
            throws CheckpointStorageException {
        List<Long> ids = new CopyOnWriteArrayList<>();
        for (String checkpointId : checkpointIdList) {
            ids.add(Long.parseLong(checkpointId));
        }
        DELETE_REQUESTS.add(new DeleteRequest(Integer.parseInt(pipelineId), ids));
        super.deleteCheckpoint(jobId, pipelineId, checkpointIdList);
    }

    @Override
    public void deleteCheckpoint(String jobId) {
        JOB_LEVEL_DELETES.incrementAndGet();
        super.deleteCheckpoint(jobId);
    }

    private static String key(String jobId, int pipelineId) {
        return jobId + ":" + pipelineId;
    }

    /** One call the coordinator made to delete checkpoints of a single pipeline. */
    public static final class DeleteRequest {

        private final int pipelineId;
        private final List<Long> checkpointIds;

        DeleteRequest(int pipelineId, List<Long> checkpointIds) {
            this.pipelineId = pipelineId;
            this.checkpointIds = checkpointIds;
        }

        public int getPipelineId() {
            return pipelineId;
        }

        public List<Long> getCheckpointIds() {
            return checkpointIds;
        }

        @Override
        public String toString() {
            return "DeleteRequest{pipelineId="
                    + pipelineId
                    + ", checkpointIds="
                    + checkpointIds
                    + '}';
        }
    }
}
