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

package org.apache.seatunnel.api.common;

/**
 * Marker interface for connectors or transforms that can distinguish whether a given exception is
 * caused by a single row (row-level error) or by a system-level failure.
 *
 * <p>This interface is used together with engine-side row error handling to decide whether a
 * failing record should be bypassed to an error output instead of failing the whole job.
 */
public interface SupportRowLevelError<T> {

    /**
     * Decide whether the given throwable represents a row-level error for the provided row.
     *
     * @param t the thrown error
     * @param row the row being processed when the error occurred
     * @return {@code true} if this is a row-level error and can be bypassed; {@code false} if this
     *     should be treated as a system-level error
     */
    boolean isRowError(Throwable t, T row);
}
