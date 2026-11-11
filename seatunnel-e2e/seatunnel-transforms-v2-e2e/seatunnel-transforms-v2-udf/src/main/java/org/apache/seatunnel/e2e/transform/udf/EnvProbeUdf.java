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

package org.apache.seatunnel.e2e.transform.udf;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.Map;

/**
 * E2E UDF that reads a value from {@code env.custom_parameters.probe} in {@link
 * ZetaUDF#open(ReadonlyConfig)} and prefixes every evaluated value with it.
 */
@AutoService(ZetaUDF.class)
public class EnvProbeUdf implements ZetaUDF {

    public static final String FUNCTION_NAME = "ENV_PROBE";

    private String envValue;

    @Override
    public String functionName() {
        return FUNCTION_NAME;
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public void open(ReadonlyConfig envOptions) {
        Map<String, String> customParameters =
                envOptions.getOptional(EnvCommonOptions.CUSTOM_PARAMETERS).orElse(null);
        envValue = customParameters == null ? null : customParameters.get("probe");
    }

    @Override
    public Object evaluate(List<Object> args) {
        return envValue + ":" + args.get(0);
    }
}
