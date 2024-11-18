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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.util;

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.catalog.ClickhouseTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig;
import org.apache.seatunnel.connectors.seatunnel.common.sql.template.SqlTemplate;

import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClickhouseCatalogUtil {

    public static String getCreateTableSql(
            String template, String database, String table, TableSchema tableSchema) {
        String primaryKey = "";
        if (tableSchema.getPrimaryKey() != null) {
            primaryKey =
                    tableSchema.getPrimaryKey().getColumnNames().stream()
                            .map(r -> "`" + r + "`")
                            .collect(Collectors.joining(","));
        }
        String uniqueKey = "";
        if (!tableSchema.getConstraintKeys().isEmpty()) {
            uniqueKey =
                    tableSchema.getConstraintKeys().stream()
                            .flatMap(c -> c.getColumnNames().stream())
                            .map(r -> "`" + r.getColumnName() + "`")
                            .collect(Collectors.joining(","));
        }
        SqlTemplate.canHandledByTemplateWithPlaceholder(
                template,
                SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder(),
                primaryKey,
                TablePath.of(database, table).getFullName(),
                ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getReplacePlaceHolder(),
                        primaryKey);
        SqlTemplate.canHandledByTemplateWithPlaceholder(
                template,
                SaveModePlaceHolder.ROWTYPE_UNIQUE_KEY.getPlaceHolder(),
                uniqueKey,
                TablePath.of(database, table).getFullName(),
                ClickhouseConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template = mergeColumnInTemplate(columnInTemplate, tableSchema, template);

        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_UNIQUE_KEY.getReplacePlaceHolder(), uniqueKey);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(column -> !columnInTemplate.containsKey(column.getName()))
                        .map(x -> columnToClickhouseType(x))
                        .collect(Collectors.joining(",\n"));

        if (template.contains(SaveModePlaceHolder.TABLE_NAME.getPlaceHolder())) {
            template =
                    template.replaceAll(
                            SaveModePlaceHolder.TABLE_NAME.getReplacePlaceHolder(), table);
        }

        return template.replaceAll(SaveModePlaceHolder.DATABASE.getReplacePlaceHolder(), database)
                .replaceAll(SaveModePlaceHolder.TABLE.getReplacePlaceHolder(), table)
                .replaceAll(
                        SaveModePlaceHolder.ROWTYPE_FIELDS.getReplacePlaceHolder(), rowTypeFields);
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template) {
        int offset = 0;
        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        List<CreateTableParser.ColumnInfo> columnInfosInSeq =
                columnInTemplate.values().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        CreateTableParser.ColumnInfo::getStartIndex))
                        .collect(Collectors.toList());
        for (CreateTableParser.ColumnInfo columnInfo : columnInfosInSeq) {
            String col = columnInfo.getName();
            if (StringUtils.isEmpty(columnInfo.getInfo())) {
                if (columnMap.containsKey(col)) {
                    Column column = columnMap.get(col);
                    String newCol = columnToClickhouseType(column);
                    String prefix = template.substring(0, columnInfo.getStartIndex() + offset);
                    String suffix = template.substring(offset + columnInfo.getEndIndex());
                    if (prefix.endsWith("`")) {
                        prefix = prefix.substring(0, prefix.length() - 1);
                        offset--;
                    }
                    if (suffix.startsWith("`")) {
                        suffix = suffix.substring(1);
                        offset--;
                    }
                    template = prefix + newCol + suffix;
                    offset += newCol.length() - columnInfo.getName().length();
                } else {
                    throw new IllegalArgumentException("Can't find column " + col + " in table.");
                }
            }
        }
        return template;
    }

    private static String columnToClickhouseType(Column column) {
        checkNotNull(column, "The column is required.");
        return String.format(
                "`%s` %s %s",
                column.getName(),
                ClickhouseTypeConverter.INSTANCE.reconvert(column).getColumnType(),
                StringUtils.isEmpty(column.getComment())
                        ? ""
                        : "COMMENT '" + column.getComment() + "'");
    }

    public static String getDropTableSql(String database, String table) {
        return "DROP TABLE IF EXISTS " + database + "." + table;
    }

    public static String getTruncateTableSql(String database, String table) {
        return "TRUNCATE TABLE " + database + "." + table;
    }

    public static String getCreateDatabaseSql(String database) {
        return "CREATE DATABASE IF NOT EXISTS " + database;
    }

    public static String getDropDatabaseSql(String database) {
        return "DROP DATABASE IF EXISTS " + database;
    }
}
