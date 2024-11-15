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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseNode;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.catalog.exception.*;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.*;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class ClickhouseCatalog implements Catalog {

    protected String defaultDatabase = "information_schema";
    private ReadonlyConfig readonlyConfig;
    private ClickhouseProxy proxy;
    private final String template;

    private String catalogName;
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseCatalog.class);

    public ClickhouseCatalog(
            ReadonlyConfig readonlyConfig,String catalogName) {
        this.readonlyConfig = readonlyConfig;
        this.catalogName = catalogName;
        this.template =readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return proxy.listDatabases();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        return proxy.listTable(databaseName);
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        List<ClickHouseColumn> clickHouseColumns = proxy.getClickHouseColumns(tablePath.getFullNameWithQuoted());

        try {

            TableSchema.Builder builder = TableSchema.builder();
            buildColumnsWithErrorCheck(
                    tablePath,
                    builder,
                    clickHouseColumns.iterator(),
                    column -> PhysicalColumn.of(
                            column.getColumnName(),
                            TypeConvertUtil.convert(column),
                            (long) column.getEstimatedLength(),
                            column.getScale(),
                            column.isNullable(),
                            null,
                            null));

            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        log.info("Create table :"+tablePath.getDatabaseName()+"."+tablePath.getTableName());
        proxy.createTable(tablePath.getDatabaseName(),tablePath.getTableName());
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        proxy.dropTable(tablePath.getDatabaseName(),tablePath.getTableName());
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if(tableExists(tablePath)){
            proxy.truncateTable(tablePath.getDatabaseName(),tablePath.getTableName());
        }
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        try {
            proxy.executeSql(sql);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed EXECUTE SQL in catalog %s", sql), e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        try {
            return proxy.isExistsData(tablePath.getFullName());
        }  catch (ExecutionException|InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        proxy.createDatabase(tablePath.getDatabaseName());
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        proxy.dropDatabase(tablePath.getDatabaseName());
    }



    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "clickhouse");
        //TODO
//        options.put("url", baseUrl + tablePath.getDatabaseName());
//        options.put("table-name", tablePath.getFullName());
//        options.put("username", username);
//        options.put("password", pwd);
        return options;
    }

    /**
     * URL has to be without database, like "jdbc:mysql://localhost:5432/" or
     * "jdbc:mysql://localhost:5432" rather than "jdbc:mysql://localhost:5432/db".
     */
    public static boolean validateJdbcUrlWithoutDatabase(String url) {
        String[] parts = url.trim().split("\\/+");

        return parts.length == 2;
    }

    /**
     * URL has to be with database, like "jdbc:mysql://localhost:5432/db" rather than
     * "jdbc:mysql://localhost:5432/".
     */
    @SuppressWarnings("MagicNumber")
    public static boolean validateJdbcUrlWithDatabase(String url) {
        String[] parts = url.trim().split("\\/+");
        return parts.length == 3;
    }



    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(readonlyConfig);
        Properties clickhouseProperties = new Properties();
        readonlyConfig
                .get(CLICKHOUSE_CONFIG)
                .forEach((key, value) -> clickhouseProperties.put(key, String.valueOf(value)));

        clickhouseProperties.put("user", readonlyConfig.get(USERNAME));
        clickhouseProperties.put("password", readonlyConfig.get(PASSWORD));
        proxy = new ClickhouseProxy(nodes.get(0));

    }

    @Override
    public void close() throws CatalogException {
         System.out.println("close clickhouse catalog");
    }

    @Override
    public String name() {
        return catalogName;
    }


    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(StringUtils.isNotBlank(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return proxy.tableExists(tablePath.getDatabaseName(),tablePath.getTableName());
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
//        if (actionType == ActionType.CREATE_TABLE) {
//            Preconditions.checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
//            return new SQLPreviewResult(
//                    StarRocksSaveModeUtil.getCreateTableSql(
//                            template,
//                            tablePath.getDatabaseName(),
//                            tablePath.getTableName(),
//                            catalogTable.get().getTableSchema()));
//        } else if (actionType == ActionType.DROP_TABLE) {
//            return new SQLPreviewResult(StarRocksSaveModeUtil.getDropTableSql(tablePath, true));
//        } else if (actionType == ActionType.TRUNCATE_TABLE) {
//            return new SQLPreviewResult(StarRocksSaveModeUtil.getTruncateTableSql(tablePath));
//        } else if (actionType == ActionType.CREATE_DATABASE) {
//            return new SQLPreviewResult(
//                    StarRocksSaveModeUtil.getCreateDatabaseSql(tablePath.getDatabaseName(), true));
//        } else if (actionType == ActionType.DROP_DATABASE) {
//            return new SQLPreviewResult(
//                    "DROP DATABASE IF EXISTS `" + tablePath.getDatabaseName() + "`");
//        } else {
//            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
//        }
        return null;
    }
}
