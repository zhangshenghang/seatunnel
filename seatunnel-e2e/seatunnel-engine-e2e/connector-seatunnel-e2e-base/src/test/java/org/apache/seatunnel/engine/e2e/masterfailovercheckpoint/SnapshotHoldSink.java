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

package org.apache.seatunnel.engine.e2e.masterfailovercheckpoint;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

/**
 * Test-only sink without committers or writer state: it discards every row and lets its writer
 * block inside {@code snapshotState(long)} on {@link SnapshotHoldGate} until the test releases it.
 */
public class SnapshotHoldSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    /** Key that binds this sink to the gate armed by the test. */
    private final String holdKey;

    public SnapshotHoldSink(String holdKey) {
        this.holdKey = holdKey;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new SnapshotHoldSinkWriter(holdKey);
    }

    @Override
    public String getPluginName() {
        return SnapshotHoldSinkFactory.IDENTIFIER;
    }
}
