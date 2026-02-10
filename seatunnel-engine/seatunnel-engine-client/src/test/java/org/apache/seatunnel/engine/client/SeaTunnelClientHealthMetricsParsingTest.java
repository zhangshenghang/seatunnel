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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SeaTunnelClientHealthMetricsParsingTest {

    @Test
    public void testParseHealthMetricsString_normal() {
        Map<String, String> parsed = SeaTunnelClient.parseHealthMetricsString("a=1, b=2, c=3");
        Assertions.assertEquals("1", parsed.get("a"));
        Assertions.assertEquals("2", parsed.get("b"));
        Assertions.assertEquals("3", parsed.get("c"));
    }

    @Test
    public void testParseHealthMetricsString_ignoreMalformedPairs() {
        Map<String, String> parsed =
                SeaTunnelClient.parseHealthMetricsString("a=1, broken, b=2, =x, c=");
        Assertions.assertEquals("1", parsed.get("a"));
        Assertions.assertEquals("2", parsed.get("b"));
        Assertions.assertEquals("", parsed.get("c"));
        Assertions.assertFalse(parsed.containsKey(""));
    }

    @Test
    public void testParseHealthMetricsString_keepCommaInsideValue() {
        Map<String, String> parsed =
                SeaTunnelClient.parseHealthMetricsString(
                        "load.process=12,34%, heap.memory.used=1,2GB, connection.count=10");
        Assertions.assertEquals("12,34%", parsed.get("load.process"));
        Assertions.assertEquals("1,2GB", parsed.get("heap.memory.used"));
        Assertions.assertEquals("10", parsed.get("connection.count"));
    }
}
