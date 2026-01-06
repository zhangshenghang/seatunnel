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

import lombok.extern.slf4j.Slf4j;

/**
 * Default row error classifier.
 *
 * <p>Current strategy:
 *
 * <ul>
 *   <li>Exception of type {@link Error} are always treated as system-level.
 *   <li>For TRANSFORM stage, non-Error throwables are treated as system-level errors by default.
 *       Transform plugins can override by implementing {@code SupportRowLevelError}.
 *   <li>For SINK stage, non-Error throwables are treated as system-level errors by default. Sink
 *       connectors can override by implementing {@code SupportRowLevelError}.
 * </ul>
 */
@Slf4j
public class DefaultRowErrorClassifier<T> implements RowErrorClassifier<T> {

    @Override
    public boolean isRowError(Throwable t, T row, RowErrorContext ctx) {
        return false;
    }
}
