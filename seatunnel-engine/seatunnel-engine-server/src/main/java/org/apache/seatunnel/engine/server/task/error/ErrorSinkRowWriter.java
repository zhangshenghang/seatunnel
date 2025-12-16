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

package org.apache.seatunnel.engine.server.task.error;

import java.io.Serializable;

/** Sink writer used by {@link ErrorHandler} to persist error records. */
public interface ErrorSinkRowWriter<T> extends Serializable, AutoCloseable {

    /**
     * Write a single error record to the error sink.
     *
     * <p>Implementations must be thread-safe in the sense of being called from the task thread.
     *
     * @param ctx error context
     * @param row original row
     * @param t the throwable that caused the error
     * @throws Exception when writing failed; the caller will treat this as a system-level error and
     *     fail the job
     */
    void write(RowErrorContext ctx, T row, Throwable t) throws Exception;

    @Override
    void close() throws Exception;
}
