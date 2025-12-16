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

package org.apache.seatunnel.engine.server.task.error;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight error handler used by engine tasks to implement row-level error counting, logging and
 * threshold checks.
 *
 * <p>Current implementation focuses on LOG/threshold behavior. ROUTE mode additionally supports
 * writing error records to a dedicated error sink when configured.
 */
@Slf4j
public class ErrorHandler<T> implements Serializable, AutoCloseable {

    private final StageErrorConfig config;
    private final ErrorSinkRowWriter<T> errorSinkWriter;

    private final AtomicLong totalRecords = new AtomicLong(0);
    private final AtomicLong errorRecords = new AtomicLong(0);

    public ErrorHandler(StageErrorConfig config) {
        this(config, null);
    }

    public ErrorHandler(StageErrorConfig config, ErrorSinkRowWriter<T> errorSinkWriter) {
        this.config = config;
        this.errorSinkWriter = errorSinkWriter;
    }

    public void incrementTotalRecords() {
        totalRecords.incrementAndGet();
    }

    public void onError(RowErrorContext ctx, T row, Throwable t) {
        long currentErrorCount = errorRecords.incrementAndGet();
        maybeThrowOnThreshold(ctx, currentErrorCount);

        if (config.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }

        try {
            String originalData =
                    config.isIncludeOriginalData()
                            ? truncate(String.valueOf(row), config.getOriginalDataMaxLength())
                            : null;

            if (config.getMode() == ErrorHandlerMode.LOG
                    || config.getMode() == ErrorHandlerMode.ROUTE) {
                log.warn(
                        "Row-level error in stage [{}], plugin [{}] on table [{}]: {}. TotalRecords={}, ErrorRecords={}, Original data: {}",
                        ctx.getStage(),
                        ctx.getPluginName(),
                        ctx.getTableId(),
                        t.getMessage(),
                        totalRecords.get(),
                        currentErrorCount,
                        originalData,
                        t);
            }

            if (config.getMode() == ErrorHandlerMode.ROUTE && errorSinkWriter != null) {
                try {
                    log.info("Writing error row to sink: {}", row);
                    errorSinkWriter.write(ctx, row, t);
                } catch (Exception sinkEx) {
                    throw new RuntimeException(
                            String.format(
                                    "Error sink failed for stage [%s], plugin [%s]",
                                    ctx.getStage(), ctx.getPluginName()),
                            sinkEx);
                }
            }
        } catch (Throwable logEx) {
            log.error(
                    "Failed to handle row-level error. stage={}, plugin={}, tableId={}, "
                            + "originalError={}, handlerFailure={}",
                    ctx != null ? ctx.getStage() : null,
                    ctx != null ? ctx.getPluginName() : null,
                    ctx != null ? ctx.getTableId() : null,
                    t != null ? t.getMessage() : null,
                    logEx.getMessage(),
                    logEx);
        }
    }

    private void maybeThrowOnThreshold(RowErrorContext ctx, long currentErrorCount) {
        long total = totalRecords.get();
        if (config.getMaxErrorRecords() > 0 && currentErrorCount > config.getMaxErrorRecords()) {
            throw new RuntimeException(
                    String.format(
                            "Too many row-level errors in stage [%s], plugin [%s]: %d records exceeded max_error_records=%d",
                            ctx.getStage(),
                            ctx.getPluginName(),
                            currentErrorCount,
                            config.getMaxErrorRecords()));
        }

        if (config.getMaxErrorRatio() > 0 && total > 0) {
            double ratio = (double) currentErrorCount / (double) total;
            if (ratio > config.getMaxErrorRatio()) {
                throw new RuntimeException(
                        String.format(
                                "Row-level error ratio in stage [%s], plugin [%s] exceeded max_error_ratio=%.4f (current=%.4f, errors=%d, total=%d)",
                                ctx.getStage(),
                                ctx.getPluginName(),
                                config.getMaxErrorRatio(),
                                ratio,
                                currentErrorCount,
                                total));
            }
        }
    }

    private String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    @Override
    public void close() {
        if (errorSinkWriter != null) {
            try {
                errorSinkWriter.close();
            } catch (Exception e) {
                log.error("Failed to close error sink writer", e);
            }
        }
    }
}
