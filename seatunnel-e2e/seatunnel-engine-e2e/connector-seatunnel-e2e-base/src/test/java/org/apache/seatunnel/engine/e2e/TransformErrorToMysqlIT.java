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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
public class TransformErrorToMysqlIT extends TestSuiteBase implements TestResource {

    private static final String MYSQL_IMAGE = "mysql:8.0.43";
    private static final String MYSQL_CONTAINER_HOST = "mysql-e2e";
    private static final String MYSQL_DATABASE = "test";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Abc!@#135_seatunnel";
    private static final int MYSQL_PORT = 3306;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                // Copy MySQL driver from local Maven repository to the SeaTunnel container
                String driverPath =
                        System.getProperty("user.home")
                                + "/.m2/repository/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

                // Create target directory
                Container.ExecResult mkdirResult =
                        container.execInContainer(
                                "bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib");
                Assertions.assertEquals(
                        0,
                        mkdirResult.getExitCode(),
                        "Failed to create directory: " + mkdirResult.getStderr());

                // Copy driver file from host to container
                container.copyFileToContainer(
                        org.testcontainers.utility.MountableFile.forHostPath(driverPath),
                        "/tmp/seatunnel/plugins/Jdbc/lib/mysql-connector-j-8.0.32.jar");
            };

    private MySQLContainer<?> mysqlContainer;

    @BeforeAll
    @Override
    public void startUp() {
        mysqlContainer =
                new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USERNAME)
                        .withPassword(MYSQL_PASSWORD)
                        .withNetwork(TestContainer.NETWORK)
                        .withNetworkAliases(MYSQL_CONTAINER_HOST)
                        .withUrlParam("allowPublicKeyRetrieval", "true");

        Startables.deepStart(Stream.of(mysqlContainer)).join();
        log.info("MySQL container started with IP: {}", mysqlContainer.getHost());

        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE orders_from_transform ("
                                + "id INT PRIMARY KEY, "
                                + "name_int INT, "
                                + "age INT)");

                statement.execute(
                        "CREATE TABLE orders_transform_error ("
                                + "error_stage VARCHAR(50), "
                                + "plugin_type VARCHAR(50), "
                                + "plugin_name VARCHAR(100), "
                                + "source_table_path VARCHAR(255), "
                                + "row_kind VARCHAR(20), "
                                + "error_type VARCHAR(50), "
                                + "error_code VARCHAR(50), "
                                + "error_message TEXT, "
                                + "exception_class VARCHAR(255), "
                                + "stacktrace TEXT, "
                                + "original_data TEXT, "
                                + "occur_time TIMESTAMP)");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create tables", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (mysqlContainer != null) {
            mysqlContainer.stop();
        }
    }

    @TestTemplate
    public void testTransformErrorRoutedToMysql(TestContainer container) throws Exception {
        // No variables needed - credentials are hardcoded in the config file
        Container.ExecResult result =
                container.executeJob("/transform_fakesource_to_mysql_with_error_handler.conf");

        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "SeaTunnel job should exit with code 0, stderr: " + result.getStderr());

        // Verify data in MySQL
        try (Connection connection =
                DriverManager.getConnection(
                        mysqlContainer.getJdbcUrl(),
                        mysqlContainer.getUsername(),
                        mysqlContainer.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                // Normal rows - expect 2 rows that passed transformation
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_from_transform");
                Assertions.assertTrue(rs.next(), "Should have count result for normal rows");
                int normalCount = rs.getInt(1);
                Assertions.assertEquals(2, normalCount, "Should have 2 normal rows in main table");

                // Error rows - expect 2 rows that failed transformation
                ResultSet ers =
                        statement.executeQuery("SELECT COUNT(*) FROM orders_transform_error");
                Assertions.assertTrue(ers.next(), "Should have count result for error rows");
                int errorCount = ers.getInt(1);
                Assertions.assertEquals(2, errorCount, "Should have 2 error rows in error table");

                log.info(
                        "Successfully verified normal count: {} and error count: {}",
                        normalCount,
                        errorCount);
            }
        } catch (SQLException e) {
            log.error("Failed to verify MySQL data", e);
            throw new RuntimeException("Failed to verify MySQL data", e);
        }
    }
}
