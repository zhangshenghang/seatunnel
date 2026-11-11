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
import org.apache.seatunnel.transform.calcite.udf.CalciteUdfContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalciteUdfEnvOptionsTest {

    @Test
    public void testEnvReachesCalciteUdfThroughFactoryUtil() {
        CalciteEnvProbeUdf.reset();

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);

        Map<String, Object> transformOptions = new HashMap<>();
        transformOptions.put(
                CalciteTransformConfig.SQL.key(),
                "select id, " + CalciteEnvProbeUdf.FUNCTION_NAME + "(name) as name from test");

        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareMultiTableTransform(
                        Collections.singletonList(catalogTable),
                        ReadonlyConfig.fromMap(transformOptions),
                        getClass().getClassLoader(),
                        "Calcite",
                        envOptions());

        // Calcite UDFs are opened while the engine is initialized for schema derivation.
        Assertions.assertTrue(CalciteEnvProbeUdf.isOpenCalled());
        Assertions.assertEquals("env-value", CalciteEnvProbeUdf.lastOpenedEnvValue());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, "hello"});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().toString());
        List<SeaTunnelRow> output =
                ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(inputRow);

        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.size());
        Assertions.assertEquals("env-value:hello", output.get(0).getField(1));
        Assertions.assertNull(CalciteUdfContext.current());
    }

    @Test
    public void testLegacyTransformWithoutEnvStillWorks() {
        CalciteEnvProbeUdf.reset();

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);

        Map<String, Object> transformOptions = new HashMap<>();
        transformOptions.put(
                CalciteTransformConfig.SQL.key(),
                "select id, " + CalciteEnvProbeUdf.FUNCTION_NAME + "(name) as name from test");

        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareMultiTableTransform(
                        Collections.singletonList(catalogTable),
                        ReadonlyConfig.fromMap(transformOptions),
                        getClass().getClassLoader(),
                        "Calcite");

        Assertions.assertTrue(CalciteEnvProbeUdf.isOpenCalled());
        Assertions.assertNull(CalciteEnvProbeUdf.lastOpenedEnvValue());

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, "hello"});
        inputRow.setTableId(catalogTable.getTableId().toTablePath().toString());
        List<SeaTunnelRow> output =
                ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(inputRow);

        Assertions.assertNotNull(output);
        Assertions.assertEquals(1, output.size());
        Assertions.assertEquals("null:hello", output.get(0).getField(1));
        Assertions.assertNull(CalciteUdfContext.current());
    }

    @Test
    public void testEnvIsIsolatedAcrossTransformsAndContextIsCleanedUp() {
        CalciteEnvProbeUdf.reset();

        SeaTunnelTransform<?> transformA = buildTransform(envOptions("env-A"));
        SeaTunnelTransform<?> transformB = buildTransform(envOptions("env-B"));

        Assertions.assertEquals("env-A:hello", flatMap(transformA, "hello").get(0).getField(1));
        Assertions.assertNull(CalciteUdfContext.current());

        Assertions.assertEquals("env-B:hello", flatMap(transformB, "hello").get(0).getField(1));
        Assertions.assertNull(CalciteUdfContext.current());

        Assertions.assertEquals("env-A:again", flatMap(transformA, "again").get(0).getField(1));
        Assertions.assertNull(CalciteUdfContext.current());
    }

    private SeaTunnelTransform<?> buildTransform(ReadonlyConfig envOptions) {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);

        Map<String, Object> transformOptions = new HashMap<>();
        transformOptions.put(
                CalciteTransformConfig.SQL.key(),
                "select id, " + CalciteEnvProbeUdf.FUNCTION_NAME + "(name) as name from test");
        return FactoryUtil.createAndPrepareMultiTableTransform(
                Collections.singletonList(catalogTable),
                ReadonlyConfig.fromMap(transformOptions),
                getClass().getClassLoader(),
                "Calcite",
                envOptions);
    }

    private static List<SeaTunnelRow> flatMap(SeaTunnelTransform<?> transform, String fieldValue) {
        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1, fieldValue});
        inputRow.setTableId(
                transform.getProducedCatalogTables().get(0).getTableId().toTablePath().toString());
        return ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(inputRow);
    }

    private static ReadonlyConfig envOptions() {
        return envOptions("env-value");
    }

    private static ReadonlyConfig envOptions(String probe) {
        Map<String, Object> customParameters = new HashMap<>();
        customParameters.put("probe", probe);
        Map<String, Object> env = new HashMap<>();
        env.put("custom_parameters", customParameters);
        return ReadonlyConfig.fromMap(env);
    }
}
