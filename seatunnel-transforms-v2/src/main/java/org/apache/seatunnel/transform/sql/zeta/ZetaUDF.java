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

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.io.Serializable;
import java.util.List;

public interface ZetaUDF extends Serializable {
    /**
     * Function name
     *
     * @return function name
     */
    String functionName();

    /**
     * The type of function result
     *
     * @param argsType input arguments type
     * @return result type
     */
    SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType);

    /**
     * Evaluate
     *
     * @param args input arguments
     * @return result value
     */
    Object evaluate(List<Object> args);

    /**
     * Whether current udf requires row level context.
     *
     * @return true means engine should call evaluateWithContext instead of evaluate
     */
    default boolean requiresContext() {
        return false;
    }

    /**
     * Evaluate with row level context.
     *
     * @param args input arguments
     * @param context row context
     * @return result value
     */
    default Object evaluateWithContext(List<Object> args, ZetaUDFContext context) {
        return evaluate(args);
    }

    /** Initialize udf resources. */
    default void open() throws Exception {}

    /**
     * Initialize udf resources with the job-level env options.
     *
     * <p>UDFs that need user-defined parameters from the job {@code env} block can override this
     * method instead of {@link #open()}. The default implementation delegates to {@link #open()} so
     * existing implementations keep working.
     *
     * @param envOptions job-level env options; never {@code null}
     */
    default void open(ReadonlyConfig envOptions) throws Exception {
        open();
    }

    /** Release udf resources. */
    default void close() {}
}
