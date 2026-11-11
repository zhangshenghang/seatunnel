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

package org.apache.seatunnel.transform.calcite.udf;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.RowKind;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Objects;

/**
 * Thread-local row context exposing per-row metadata (table id, {@link RowKind}) and job-level env
 * options to Calcite UDFs.
 *
 * <p>UDFs read it via {@link #current()}; the engine opens a {@link Scope} around each row.
 */
@Slf4j
public final class CalciteUdfContext {

    private static final ThreadLocal<CalciteUdfContext> HOLDER = new ThreadLocal<>();

    private @Nullable String rawTableId;
    private boolean tablePathResolved;
    private @Nullable String database;
    private @Nullable String schema;
    private @Nullable String table;
    private @Nullable RowKind rowKind;
    private ReadonlyConfig envOptions = ReadonlyConfig.fromMap(Collections.emptyMap());

    private CalciteUdfContext() {}

    /**
     * Returns the context for the row currently being processed by the Calcite engine. Returns
     * {@code null} when called outside of a UDF execution scope.
     */
    @Nullable public static CalciteUdfContext current() {
        return HOLDER.get();
    }

    /**
     * Opens a UDF execution scope bound to the current thread, populated with the given row
     * metadata. The returned handle MUST be closed (preferably via try-with-resources) so the
     * thread-local context is properly torn down.
     *
     * <p>If a scope already exists on this thread (e.g. nested SQL transforms), the existing
     * context instance is reused and its metadata is refreshed; nested scopes do not tear down
     * their parent.
     */
    public static Scope enter(@Nullable String tableId, @Nullable RowKind rowKind) {
        return enter(tableId, rowKind, ReadonlyConfig.fromMap(Collections.emptyMap()));
    }

    /**
     * Opens a UDF execution scope bound to the current thread, populated with the given row
     * metadata and job-level env options.
     *
     * <p>If a scope already exists on this thread (e.g. nested SQL transforms), the existing
     * context instance is reused and its metadata and env options are refreshed; nested scopes do
     * not tear down their parent.
     */
    public static Scope enter(
            @Nullable String tableId,
            @Nullable RowKind rowKind,
            @Nullable ReadonlyConfig envOptions) {
        CalciteUdfContext existing = HOLDER.get();
        if (existing != null) {
            existing.update(tableId, rowKind, envOptions);
            return Scope.NOOP;
        }
        CalciteUdfContext fresh = new CalciteUdfContext();
        fresh.update(tableId, rowKind, envOptions);
        HOLDER.set(fresh);
        return HOLDER::remove;
    }

    private void update(
            @Nullable String tableId,
            @Nullable RowKind rowKind,
            @Nullable ReadonlyConfig envOptions) {
        this.rowKind = rowKind;
        this.envOptions =
                envOptions == null ? ReadonlyConfig.fromMap(Collections.emptyMap()) : envOptions;
        updateTableId(tableId);
    }

    private void updateTableId(@Nullable String tableId) {
        if (Objects.equals(this.rawTableId, tableId)) {
            return;
        }
        this.rawTableId = tableId;
        this.database = null;
        this.schema = null;
        this.table = null;
        this.tablePathResolved = false;
    }

    private void resolveTablePathIfNeeded() {
        if (tablePathResolved) {
            return;
        }
        tablePathResolved = true;
        if (rawTableId == null) {
            return;
        }
        try {
            TablePath tablePath = TablePath.of(rawTableId);
            this.database = tablePath.getDatabaseName();
            this.schema = tablePath.getSchemaName();
            this.table = tablePath.getTableName();
        } catch (IllegalArgumentException e) {
            log.warn(
                    "Failed to parse tableId '{}' as TablePath, "
                            + "getDatabase()/getSchema()/getTable() will return null",
                    rawTableId,
                    e);
        }
    }

    @Nullable public String getRawTableId() {
        return rawTableId;
    }

    @Nullable public String getDatabase() {
        resolveTablePathIfNeeded();
        return database;
    }

    @Nullable public String getSchema() {
        resolveTablePathIfNeeded();
        return schema;
    }

    @Nullable public String getTable() {
        resolveTablePathIfNeeded();
        return table;
    }

    @Nullable public RowKind getRowKind() {
        return rowKind;
    }

    /**
     * Job-level env options; never {@code null}. Returns an empty config when no env was supplied.
     */
    public ReadonlyConfig getEnvOptions() {
        return envOptions;
    }

    /**
     * Auto-closeable handle returned by {@link #enter}. Closing it removes the thread-local context
     * if this scope owns it.
     */
    public interface Scope extends AutoCloseable {

        /** No-op scope used for nested {@link #enter} calls. */
        Scope NOOP = () -> {};

        @Override
        void close();
    }
}
