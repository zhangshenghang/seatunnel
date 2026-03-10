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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.StringSplitStrategy;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcSourceConfigTest {

    @Test
    public void testResolveDynamicDefaultStringSplitStrategy() {
        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(baseConfig()));
        assertEquals(StringSplitStrategy.RANGE, sourceConfig.getStringSplitStrategy());
    }

    @Test
    public void testResolveOldVersionDefaultStringSplitStrategy() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("query", "select * from test");
        configMap.put("partition_column", "name");

        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals(StringSplitStrategy.HASH, sourceConfig.getStringSplitStrategy());
    }

    @Test
    public void testResolveOldVersionCharsetBasedStringSplitStrategy() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("query", "select * from test");
        configMap.put("partition_column", "name");
        configMap.put("split.string_split_mode", "charset_based");

        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals(StringSplitStrategy.RANGE, sourceConfig.getStringSplitStrategy());
    }

    @Test
    public void testResolveLegacyHashFlag() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("split.enable-hash-split-for-string-column", true);

        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals(StringSplitStrategy.HASH, sourceConfig.getStringSplitStrategy());

        configMap.put("split.enable-hash-split-for-string-column", false);
        sourceConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals(StringSplitStrategy.NONE, sourceConfig.getStringSplitStrategy());
    }

    @Test
    public void testExplicitStrategyOverridesLegacyFlag() {
        Map<String, Object> configMap = baseConfig();
        configMap.put("split.string-strategy", "range");
        configMap.put("split.enable-hash-split-for-string-column", false);

        JdbcSourceConfig sourceConfig = JdbcSourceConfig.of(ReadonlyConfig.fromMap(configMap));
        assertEquals(StringSplitStrategy.RANGE, sourceConfig.getStringSplitStrategy());
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:postgresql://localhost:5432/test");
        configMap.put("driver", "org.postgresql.Driver");
        configMap.put("table_path", "public.test");
        return configMap;
    }
}
