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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import org.apache.seatunnel.transform.calcite.udf.CalciteUdfContext;

import com.google.auto.service.AutoService;

/** Test-only Calcite UDF used to verify env propagation through the job config parser. */
@AutoService(CalciteUdf.class)
public class ParserCalciteEnvProbeUdf implements CalciteUdf {

    public static final String FUNCTION_NAME = "PARSER_CALCITE_ENV_PROBE";

    @Override
    public String functionName() {
        return FUNCTION_NAME;
    }

    public static String eval(String value) {
        CalciteUdfContext context = CalciteUdfContext.current();
        String envValue = null;
        if (context != null) {
            envValue =
                    context.getEnvOptions()
                            .getOptional(Options.key("probe").stringType().noDefaultValue())
                            .orElse(null);
        }
        return envValue + ":" + value;
    }

    @Override
    public void open(ReadonlyConfig envOptions) {
        // Stateless on purpose; eval reads the per-row context.
    }
}
