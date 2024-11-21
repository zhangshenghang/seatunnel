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

package org.apache.seatunnel.e2e.connector.hive;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.seatunnel.e2e.common.container.TestContainer.NETWORK;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Slf4j
public class HiveKerberosIT extends TestSuiteBase implements TestResource {
    private static final String CREATE_SQL =
            "CREATE TABLE test_hive_sink_on_hdfs_with_kerberos"
                    + "("
                    + "    pk_id  BIGINT,"
                    + "    name   STRING,"
                    + "    score  INT"
                    + ")";

    private static final String HMS_HOST = "metastore";
    private static final String HIVE_SERVER_HOST = "hiveserver2";
    private GenericContainer<?> hbaseContainer;

    private String hiveExeUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar";
    }

    private String libFb303Url() {
        return "https://repo1.maven.org/maven2/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar";
    }

    private String hadoopAwsUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";
    }

    private String aliyunSdkOssUrl() {
        return "https://repo1.maven.org/maven2/com/aliyun/oss/aliyun-sdk-oss/3.4.1/aliyun-sdk-oss-3.4.1.jar";
    }

    private String jdomUrl() {
        return "https://repo1.maven.org/maven2/org/jdom/jdom/1.1/jdom-1.1.jar";
    }

    private String hadoopAliyunUrl() {
        return "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aliyun/3.1.4/hadoop-aliyun-3.1.4.jar";
    }

    private String hadoopCosUrl() {
        return "https://repo1.maven.org/maven2/com/qcloud/cos/hadoop-cos/2.6.5-8.0.2/hadoop-cos-2.6.5-8.0.2.jar";
    }

    private HiveContainer hiveServerContainer;
    private HiveContainer hmsContainer;
    private Connection hiveConnection;
    private String pluginHiveDir = "/tmp/seatunnel/plugins/Hive/lib";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // The jar of hive-exec
                Container.ExecResult downloadHiveExeCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p "
                                        + pluginHiveDir
                                        + " && cd "
                                        + pluginHiveDir
                                        + " && wget "
                                        + hiveExeUrl());
                Assertions.assertEquals(
                        0,
                        downloadHiveExeCommands.getExitCode(),
                        downloadHiveExeCommands.getStderr());
                Container.ExecResult downloadLibFb303Commands =
                        container.execInContainer(
                                "sh", "-c", "cd " + pluginHiveDir + " && wget " + libFb303Url());
                Assertions.assertEquals(
                        0,
                        downloadLibFb303Commands.getExitCode(),
                        downloadLibFb303Commands.getStderr());
                // The jar of s3
                Container.ExecResult downloadS3Commands =
                        container.execInContainer(
                                "sh", "-c", "cd " + pluginHiveDir + " && wget " + hadoopAwsUrl());
                Assertions.assertEquals(
                        0, downloadS3Commands.getExitCode(), downloadS3Commands.getStderr());
                // The jar of oss
                Container.ExecResult downloadOssCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "cd "
                                        + pluginHiveDir
                                        + " && wget "
                                        + aliyunSdkOssUrl()
                                        + " && wget "
                                        + jdomUrl()
                                        + " && wget "
                                        + hadoopAliyunUrl());
                Assertions.assertEquals(
                        0, downloadOssCommands.getExitCode(), downloadOssCommands.getStderr());
                // The jar of cos
                Container.ExecResult downloadCosCommands =
                        container.execInContainer(
                                "sh", "-c", "cd " + pluginHiveDir + " && wget " + hadoopCosUrl());
                Assertions.assertEquals(
                        0, downloadCosCommands.getExitCode(), downloadCosCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        hbaseContainer =
                new GenericContainer<>("kerberos-server6:latest")
                        .withNetwork(NETWORK)
                        .withExposedPorts(88,749)
                        .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kerberos"))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("kerberos-server6:latest")));
        hbaseContainer.setPortBindings(Arrays.asList("88/udp:88/udp", "749:749"));
        Startables.deepStart(Stream.of(hbaseContainer)).join();
        given().ignoreExceptions()
            .await()
            .atMost(30, TimeUnit.SECONDS)
            .pollDelay(Duration.ofSeconds(1L))
            .untilAsserted(() -> hbaseContainer.copyFileFromContainer("/tmp/hive.keytab", "/tmp/hive.keytab"));



        hmsContainer =
                HiveContainer.hmsStandalone()
                        .withCreateContainerCmdModifier(cmd -> cmd.withName(HMS_HOST))
                        .withNetwork(NETWORK)
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/krb5.conf").getPath(),
                                "/etc/krb5.conf")
                        .withFileSystemBind("/tmp/hive.keytab", "/tmp/hive.keytab")
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/hive-site.xml").getPath(),
                                "/opt/hive/conf/hive-site.xml")
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/core-site.xml").getPath(),
                                "/opt/hadoop/etc/hadoop/core-site.xml")
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/core-site.xml").getPath(),
                                "/opt/hive/conf/core-site.xml")
                        .withNetworkAliases(HMS_HOST);
        hmsContainer.setPortBindings(Collections.singletonList("9083:9083"));


        Startables.deepStart(Stream.of(hmsContainer)).join();
        log.info("HMS just started");

        hiveServerContainer =
                HiveContainer.hiveServer()
                        .withNetwork(NETWORK)
                        .withCreateContainerCmdModifier(cmd -> cmd.withName(HIVE_SERVER_HOST))
                        .withNetworkAliases(HIVE_SERVER_HOST)
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/krb5.conf").getPath(),
                                "/etc/krb5.conf")
                        .withFileSystemBind("/tmp/hive.keytab", "/tmp/hive.keytab")
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/hive-site.xml").getPath(),
                                "/opt/hive/conf/hive-site.xml")
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/core-site.xml").getPath(),
                                "/opt/hadoop/etc/hadoop/core-site.xml")
                        .withFileSystemBind(ContainerUtil.getResourcesFile("/kerberos/core-site.xml").getPath(),
                                "/opt/hive/conf/core-site.xml")
                        .withFileSystemBind("/tmp/data", "/opt/hive/data")
                        .withEnv("SERVICE_OPTS", "-Dhive.metastore.uris=thrift://metastore:9083 -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true")
                        .withEnv("IS_RESUME", "true")
//                        .withCommand("/bin/bash", "-c", "chmod 777 /opt/hive/conf/hive.keytab")
                        .dependsOn(hmsContainer);
        hiveServerContainer.setPortBindings(Collections.singletonList("10000:10000"));

        Startables.deepStart(Stream.of(hiveServerContainer)).join();
        log.info("HiveServer2 just started");
        given().ignoreExceptions()
                .await()
                .atMost(3600, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(10L))
                .pollInterval(Duration.ofSeconds(3L))
                .untilAsserted(this::initializeConnection);
        prepareTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (hmsContainer != null) {
            log.info(hmsContainer.execInContainer("cat", "/tmp/hive/hive.log").getStdout());
            hmsContainer.close();
        }
        if (hiveServerContainer != null) {
            log.info(hiveServerContainer.execInContainer("cat", "/tmp/hive/hive.log").getStdout());
            hiveServerContainer.close();
        }
    }

    private void initializeConnection()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                    SQLException {
        this.hiveConnection = this.hiveServerContainer.getConnection(true);
    }

    private void prepareTable() throws Exception {
        log.info(
                String.format(
                        "Databases are %s",
                        this.hmsContainer.createMetaStoreClient(true).getAllDatabases()));
        try (Statement statement = this.hiveConnection.createStatement()) {
            statement.execute(CREATE_SQL);
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw exception;
        }
    }

    private void executeJob(TestContainer container, String job1, String job2)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob(job1);
        Assertions.assertEquals(0, execResult.getExitCode());

        Container.ExecResult readResult = container.executeJob(job2);
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @TestTemplate
    public void testFakeSinkHiveOnHDFS(TestContainer container) throws Exception {
        container.copyAbsolutePathToContainer("/tmp/hive.keytab", "/tmp/hive.keytab");
        container.copyFileToContainer("/kerberos/krb5.conf","/tmp/krb5.conf");
        container.copyFileToContainer("/kerberos/hive-site.xml","/tmp/hive-site.xml");

        Container.ExecResult execResult = container.executeJob("/fake_to_hive_on_hdfs_with_kerberos.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Container.ExecResult readResult = container.executeJob("/hive_on_hdfs_to_assert_with_kerberos.conf");
        Assertions.assertEquals(0, readResult.getExitCode());

        Container.ExecResult execResult2 = container.executeJob("/fake_to_hive_on_hdfs.conf");
        Assertions.assertEquals(1, execResult2.getExitCode());
        Assertions.assertTrue(execResult2.getStderr().contains("Get hive table information from hive metastore service failed"));
        Container.ExecResult readResult2 = container.executeJob("/hive_on_hdfs_to_assert.conf");
        Assertions.assertEquals(1, readResult2.getExitCode());
        Assertions.assertTrue(readResult2.getStderr().contains("Get hive table information from hive metastore service failed"));
    }


    @TestTemplate
    @Disabled(
            "[HDFS/COS/OSS/S3] is not available in CI, if you want to run this test, please set up your own environment in the test case file, hadoop_hive_conf_path_local and ip below}")
    public void testFakeSinkHiveOnS3(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_s3.conf", "/hive_on_s3_to_assert.conf");
    }

    @TestTemplate
    @Disabled(
            "[HDFS/COS/OSS/S3] is not available in CI, if you want to run this test, please set up your own environment in the test case file, hadoop_hive_conf_path_local and ip below}")
    public void testFakeSinkHiveOnOSS(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_oss.conf", "/hive_on_oss_to_assert.conf");
    }

    @TestTemplate
    @Disabled(
            "[HDFS/COS/OSS/S3] is not available in CI, if you want to run this test, please set up your own environment in the test case file, hadoop_hive_conf_path_local and ip below}")
    public void testFakeSinkHiveOnCos(TestContainer container) throws Exception {
        executeJob(container, "/fake_to_hive_on_cos.conf", "/hive_on_cos_to_assert.conf");
    }
}
