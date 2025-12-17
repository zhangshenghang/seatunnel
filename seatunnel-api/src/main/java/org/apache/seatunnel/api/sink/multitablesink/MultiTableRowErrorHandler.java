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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * Callback interface that allows engine implementations to intercept row-level errors emitted from
 * sub writers of {@link MultiTableSinkWriter}.
 *
 * <p>Implementations can decide whether the error is fully handled (for example, routed to an error
 * sink) or should be treated as fatal for the job.
 */
@FunctionalInterface
public interface MultiTableRowErrorHandler {

    /**
     * Handle a row-level error produced by a concrete sink writer.
     *
     * @param writer the concrete sink writer that threw the error
     * @param tableId logical table identifier associated with the row; may be {@code null}
     * @param row the row that failed
     * @param t the exception thrown from the sink writer
     * @return {@code true} if the error has been handled and the writer thread can continue; {@code
     *     false} if the error should be treated as fatal and stop processing.
     */
    boolean handleRowError(
            SinkWriter<SeaTunnelRow, ?, ?> writer, String tableId, SeaTunnelRow row, Throwable t);
}
