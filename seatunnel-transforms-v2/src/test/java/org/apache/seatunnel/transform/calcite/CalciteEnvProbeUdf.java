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

package org.apache.seatunnel.transform.calcite;

import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.transform.calcite.udf.CalciteUdf;
import org.apache.seatunnel.transform.calcite.udf.CalciteUdfContext;

import com.google.auto.service.AutoService;

/**
 * Test-only Calcite UDF that captures the env options passed to {@link
 * CalciteUdf#open(ReadonlyConfig)} and reads them from {@link CalciteUdfContext} in {@code eval}.
 */
@AutoService(CalciteUdf.class)
public class CalciteEnvProbeUdf implements CalciteUdf {

    public static final String FUNCTION_NAME = "TEST_CALCITE_ENV_PROBE";

    private static volatile String lastOpenedEnvValue;
    private static volatile boolean openCalled;

    public static void reset() {
        lastOpenedEnvValue = null;
        openCalled = false;
    }

    public static String lastOpenedEnvValue() {
        return lastOpenedEnvValue;
    }

    public static boolean isOpenCalled() {
        return openCalled;
    }

    @Override
    public String functionName() {
        return FUNCTION_NAME;
    }

    @Override
    public void open(ReadonlyConfig envOptions) {
        openCalled = true;
        lastOpenedEnvValue = probeValue(envOptions);
    }

    public static String eval(String value) {
        CalciteUdfContext context = CalciteUdfContext.current();
        String probe = context == null ? null : probeValue(context.getEnvOptions());
        return probe + ":" + value;
    }

    private static String probeValue(ReadonlyConfig envOptions) {
        return envOptions
                .getOptional(Options.key("probe").stringType().noDefaultValue())
                .orElse(null);
    }
}
