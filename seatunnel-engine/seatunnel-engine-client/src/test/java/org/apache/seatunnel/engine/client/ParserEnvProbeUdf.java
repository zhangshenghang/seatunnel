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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import com.google.auto.service.AutoService;

import java.util.List;

/** Test-only UDF used to verify env propagation through the job config parser. */
@AutoService(ZetaUDF.class)
public class ParserEnvProbeUdf implements ZetaUDF {

    public static final String FUNCTION_NAME = "PARSER_ENV_PROBE";

    private static volatile String lastOpenedEnvValue;

    private String envValue;

    public static void reset() {
        lastOpenedEnvValue = null;
    }

    public static String lastOpenedEnvValue() {
        return lastOpenedEnvValue;
    }

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
        envValue =
                envOptions
                        .getOptional(Options.key("probe").stringType().noDefaultValue())
                        .orElse(null);
        lastOpenedEnvValue = envValue;
    }

    @Override
    public Object evaluate(List<Object> args) {
        return envValue + ":" + args.get(0);
    }
}
