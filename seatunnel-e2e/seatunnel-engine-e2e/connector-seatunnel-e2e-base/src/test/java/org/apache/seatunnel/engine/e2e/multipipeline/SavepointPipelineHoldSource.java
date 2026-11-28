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

package org.apache.seatunnel.engine.e2e.multipipeline;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test-only bounded BATCH source whose reader can be held at the end of its input.
 *
 * <p>It exists for the multi-pipeline savepoint tests: a bounded pipeline that never signals the
 * end of its input stays {@code RUNNING} for as long as the test needs, without relying on a finite
 * read delay, and the test releases it explicitly through {@link SavepointPipelineHoldGate} when
 * the pipeline is supposed to finish.
 */
@AutoService(SeaTunnelSource.class)
public class SavepointPipelineHoldSource
        implements SeaTunnelSource<
                SeaTunnelRow, SavepointPipelineHoldSplit, SavepointPipelineHoldState> {

    /** Prefix of the generated row names, only used to make the sink output readable. */
    static final String ROW_PREFIX = "hold-";

    private ReadonlyConfig config;

    public SavepointPipelineHoldSource() {}

    public SavepointPipelineHoldSource(ReadonlyConfig config) {
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return SavepointPipelineHoldSourceFactory.IDENTIFIER;
    }

    @Override
    @Deprecated
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(
                CatalogTable.of(
                        TableIdentifier.of("e2e", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        new PhysicalColumn(
                                                "id",
                                                BasicType.LONG_TYPE,
                                                null,
                                                null,
                                                false,
                                                null,
                                                null))
                                .column(
                                        new PhysicalColumn(
                                                "name",
                                                BasicType.STRING_TYPE,
                                                null,
                                                null,
                                                false,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        SavepointPipelineHoldSourceFactory.IDENTIFIER));
    }

    @Override
    public SourceReader<SeaTunnelRow, SavepointPipelineHoldSplit> createReader(
            SourceReader.Context readerContext) {
        return new SavepointPipelineHoldSourceReader(
                readerContext,
                config.get(SavepointPipelineHoldSourceFactory.HOLD_KEY),
                config.get(SavepointPipelineHoldSourceFactory.RECORDS_PER_POLL),
                config.get(SavepointPipelineHoldSourceFactory.IDLE_SLEEP_MS));
    }

    @Override
    public SourceSplitEnumerator<SavepointPipelineHoldSplit, SavepointPipelineHoldState>
            createEnumerator(
                    SourceSplitEnumerator.Context<SavepointPipelineHoldSplit> enumeratorContext) {
        return new SavepointPipelineHoldSplitEnumerator(
                enumeratorContext, createInitialSplits(), false);
    }

    @Override
    public SourceSplitEnumerator<SavepointPipelineHoldSplit, SavepointPipelineHoldState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<SavepointPipelineHoldSplit> enumeratorContext,
                    SavepointPipelineHoldState checkpointState) {
        return new SavepointPipelineHoldSplitEnumerator(
                enumeratorContext,
                checkpointState.getPendingSplits(),
                !checkpointState.getPendingSplits().isEmpty());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    private List<SavepointPipelineHoldSplit> createInitialSplits() {
        int splitNum = Math.max(1, config.get(SavepointPipelineHoldSourceFactory.SPLIT_NUM));
        int rowNum = Math.max(1, config.get(SavepointPipelineHoldSourceFactory.ROW_NUM));
        long rowsPerSplit = Math.max(1L, rowNum / splitNum);
        List<SavepointPipelineHoldSplit> splits = new ArrayList<>(splitNum);
        long nextRow = 0L;
        for (int splitIndex = 0; splitIndex < splitNum; splitIndex++) {
            long endRow =
                    splitIndex == splitNum - 1 ? rowNum : Math.min(rowNum, nextRow + rowsPerSplit);
            splits.add(new SavepointPipelineHoldSplit("split-" + splitIndex, nextRow, endRow));
            nextRow = endRow;
        }
        return splits;
    }
}
