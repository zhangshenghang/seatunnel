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
        if (config.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }
        totalRecords.incrementAndGet();
    }

    public void onError(RowErrorContext ctx, T row, Throwable t) {
        if (config.getMode() == ErrorHandlerMode.DISABLE) {
            return;
        }

        long currentErrorCount = errorRecords.incrementAndGet();
        maybeThrowOnThreshold(ctx, currentErrorCount);

        // Build original data safely; failures here should not kill the job.
        String originalData = null;
        if (config.isIncludeOriginalData()) {
            try {
                originalData = truncate(String.valueOf(row), config.getOriginalDataMaxLength());
            } catch (Throwable buildEx) {
                if (buildEx instanceof Error) {
                    throw (Error) buildEx;
                }
                log.error(
                        "Failed to build original_data for row-level error. stage={}, plugin={}, tableId={}, originalError={}",
                        ctx != null ? ctx.getStage() : null,
                        ctx != null ? ctx.getPluginName() : null,
                        ctx != null ? ctx.getTableId() : null,
                        t != null ? t.getMessage() : null,
                        buildEx);
            }
        }

        // Always try to log the row-level error when LOG / ROUTE is enabled.
        if (config.getMode() == ErrorHandlerMode.LOG
                || config.getMode() == ErrorHandlerMode.ROUTE) {
            try {
                String stage = ctx != null ? ctx.getStage() : null;
                String pluginName = ctx != null ? ctx.getPluginName() : null;
                String tableId = ctx != null ? ctx.getTableId() : null;
                String errorMessage = t != null ? t.getMessage() : null;

                if (config.isIncludeStacktrace() && t != null) {
                    log.warn(
                            "Row-level error in stage [{}], plugin [{}] on table [{}]: {}. TotalRecords={}, ErrorRecords={}, Original data: {}",
                            stage,
                            pluginName,
                            tableId,
                            errorMessage,
                            totalRecords.get(),
                            currentErrorCount,
                            originalData,
                            t);
                } else {
                    log.warn(
                            "Row-level error in stage [{}], plugin [{}] on table [{}]: {}. TotalRecords={}, ErrorRecords={}, Original data: {}",
                            stage,
                            pluginName,
                            tableId,
                            errorMessage,
                            totalRecords.get(),
                            currentErrorCount,
                            originalData);
                }
            } catch (Throwable logEx) {
                if (logEx instanceof Error) {
                    throw (Error) logEx;
                }
                log.error(
                        "Failed to log row-level error. stage={}, plugin={}, tableId={}, originalError={}, logFailure={}",
                        ctx != null ? ctx.getStage() : null,
                        ctx != null ? ctx.getPluginName() : null,
                        ctx != null ? ctx.getTableId() : null,
                        t != null ? t.getMessage() : null,
                        logEx.getMessage(),
                        logEx);
            }
        }

        // In ROUTE mode, delegate to the error sink. For queue_overflow_policy = FAIL we
        // propagate sink failures to fail the job; for DROP/BLOCK we only log and continue.
        if (config.getMode() == ErrorHandlerMode.ROUTE && errorSinkWriter != null) {
            try {
                log.info("Writing error row to sink: {}", row);
                errorSinkWriter.write(ctx, row, t);
            } catch (Exception sinkEx) {
                if (config.getQueueOverflowPolicy() == QueueOverflowPolicy.FAIL) {
                    throw new RuntimeException(
                            String.format(
                                    "Error sink failed for stage [%s], plugin [%s]",
                                    ctx.getStage(), ctx.getPluginName()),
                            sinkEx);
                }
                log.error(
                        "Error sink failed for stage [{}], plugin [{}] with queue_overflow_policy={}, "
                                + "job will continue running",
                        ctx.getStage(),
                        ctx.getPluginName(),
                        config.getQueueOverflowPolicy(),
                        sinkEx);
            }
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

        // Error ratio can be very unstable for tiny samples, so we only enable ratio checks after
        // a configurable warm-up threshold (max_error_ratio_min_records).
        int minTotalForRatio =
                config.getMaxErrorRatioMinRecords() > 0 ? config.getMaxErrorRatioMinRecords() : 1;
        if (config.getMaxErrorRatio() > 0 && total >= minTotalForRatio) {
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
        if (value == null) {
            return null;
        }
        if (maxLength <= 0) {
            return "";
        }
        if (value.length() <= maxLength) {
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
