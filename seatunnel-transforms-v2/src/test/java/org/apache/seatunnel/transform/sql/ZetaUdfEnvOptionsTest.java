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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZetaUdfEnvOptionsTest {

    @Test
    public void testEnvReachesZetaUdfThroughFactoryUtil() {
        ZetaEnvProbeUdf.reset();

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);

        Map<String, Object> transformOptions = new HashMap<>();
        transformOptions.put(
                SQLTransform.KEY_QUERY.key(),
                "select " + ZetaEnvProbeUdf.FUNCTION_NAME + "(name) as name from test");
        ReadonlyConfig options = ReadonlyConfig.fromMap(transformOptions);

        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareMultiTableTransform(
                        Collections.singletonList(catalogTable),
                        options,
                        getClass().getClassLoader(),
                        "Sql",
                        envOptions());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, "hello"});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().toString());
        List<SeaTunnelRow> output =
                ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(inputRow);

        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.size());
        Assertions.assertTrue(ZetaEnvProbeUdf.isOpenCalled());
        Assertions.assertEquals("env-value", ZetaEnvProbeUdf.lastOpenedEnvValue());
        Assertions.assertEquals("env-value:hello", output.get(0).getField(0));
    }

    @Test
    public void testLegacyTransformWithoutEnvStillWorks() {
        ZetaEnvProbeUdf.reset();

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);

        Map<String, Object> transformOptions = new HashMap<>();
        transformOptions.put(
                SQLTransform.KEY_QUERY.key(),
                "select " + ZetaEnvProbeUdf.FUNCTION_NAME + "(name) as name from test");
        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareMultiTableTransform(
                        Collections.singletonList(catalogTable),
                        ReadonlyConfig.fromMap(transformOptions),
                        getClass().getClassLoader(),
                        "Sql");

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, "hello"});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().toString());
        List<SeaTunnelRow> output =
                ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(inputRow);

        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.size());
        Assertions.assertTrue(ZetaEnvProbeUdf.isOpenCalled());
        Assertions.assertNull(ZetaEnvProbeUdf.lastOpenedEnvValue());
        Assertions.assertEquals("null:hello", output.get(0).getField(0));
    }

    private static ReadonlyConfig envOptions() {
        Map<String, Object> customParameters = new HashMap<>();
        customParameters.put("probe", "env-value");
        Map<String, Object> env = new HashMap<>();
        env.put("custom_parameters", customParameters);
        return ReadonlyConfig.fromMap(env);
    }
}
