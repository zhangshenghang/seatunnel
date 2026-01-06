# Error Handling (Experimental)

In SeaTunnel, the default behavior is simple: if a connector or transform throws an exception, the job fails.

Starting from this experimental feature, you can change this behavior and let the engine **capture bad records, route them to an error sink, and keep the job running**.

This page introduces the basic concepts, how it works at a high level, and the most important caveats you must know before enabling it.

> **Status: Experimental**
>
> Error handling and row-level routing are disabled by default. The
> API and behavior may change in future versions.

## When Do I Need Error Handling?

Typical cases where this feature helps:

- A few dirty records in a large batch job (for example, invalid dates, strings that are too long).
- Occasional primary-key conflicts in a sink table.
- You want the **job to keep running**, and handle bad records later from a separate error table or log.

Cases where you probably **should not** enable it (or must be very careful):

- You require strong at-least-once / exactly-once guarantees for every valid record.
- You use complex multi-table sinks and need strict cross-table consistency.

## High-Level Idea

With error handling enabled, the engine will, for each record:

1. Let the transform / sink process the record as usual.
2. If an exception is thrown, decide whether it is:
   - a **row-level error** (caused by this single record), or
   - a **system-level error** (connection down, OOM, etc.).
3. If it is system-level, fail the job like before.
4. If it is row-level, send the record and error information to an **ErrorHandler**:
   - In `LOG` mode it only logs.
   - In `ROUTE` mode it writes the record and error details to a configured **error sink** (for example, a JDBC error table).

Other good records keep flowing.

You configure this behavior via:

- stage-level options such as `env.sink_error_handler` / `env.transform_error_handler`, or
- a global `env.error_handler`.

## Key Concepts (User View)

### Modes

At the config level you will mainly see a `mode` field:

- `DISABLE` – turn off error handling for this stage (default).
- `LOG` – log row-level errors, but do not route them to an error sink.
- `ROUTE` – log errors and write them to an error sink.

If you do **not** configure any error handler, behavior stays exactly as before: any exception fails the job.

### Error Sink

An **error sink** is just another sink that receives error records. You configure it under `..._error_handler.sink`, for example:

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"

    sink {
      plugin_name = "Jdbc"
      error_table = "orders_sink_error_basic"
      # other Jdbc sink options
    }
  }
}
```

Typical patterns:

- Main sink writes to `orders_from_sink`.
- Error sink writes to `orders_sink_error_*` tables, one or several.

### Row-Level vs System-Level

The engine decides whether an exception should be treated as:

- a **row-level error** (skip this record and continue), or
- a **system-level error** (fail the job).

Current default behavior (important):

- **Sink stage**: If a sink connector does not implement `SupportRowLevelError`, its exceptions will be treated as system-level failures.
- **Transform stage**: If a transform does not implement `SupportRowLevelError`, its exceptions will be treated as system-level failures.

For connectors (for example JDBC), the connector itself can explicitly declare what it considers a row-level error (through `SupportRowLevelError`). The engine uses that information first, then falls back to a generic classifier.

> Note
>
> This page describes the current **generic engine-level flow**. Over time, more built-in transforms
> are expected to implement `SupportRowLevelError` to provide more accurate row-level vs system-level
> classification.

### Transform Row-Level Errors (Important)

When a row-level error happens in a transform, the failing record is dropped from the main pipeline:

- `map(...)` errors result in `null` output (record filtered).
- `flatMap(...)` errors result in an empty output list (record dropped).

This means the record will not reach downstream transforms or sinks. If `mode = ROUTE` is enabled with an error sink, the original record and error information can still be written to the error sink.

## How JDBC Error Handling Works (Important)

JDBC is the first connector that makes heavy use of row-level error handling.

### What Is Considered a Row-Level Error in JDBC?

`JdbcSinkWriter` checks the `SQLException` chain and treats an error as **row-level** when the SQLState:

- starts with `22` – data exception (for example, data too long), or
- starts with `23` – integrity constraint violation (for example, duplicate key).

If no such SQLState is found, the error is treated as **system-level**, and the job fails.

### What Happens to Batches?

JDBC sinks usually buffer multiple records in a JDBC batch before flushing them to the database.

When a row-level error happens **during write**:

- The connector catches the exception.
- If it decides this is a row-level data error, it calls a helper to **clear the current JDBC batch in memory**.

This means:

- All records that were added to the current batch, but not yet sent to the database, are cleared.
- The bad record will be routed to the error handler (log / error table).
- The other records in the same batch will **not** be automatically retried.

From a user perspective this is equivalent to:

> **"When a row-level error appears, the whole batch is treated as an error batch."**

So in **batching + error handling** scenarios:

- You may lose a small number of otherwise valid records.
- Strict at-least-once semantics for every valid record are **not guaranteed**.

This behavior is connector-specific, and future versions may optimize per sink implementation to reduce the chance of losing valid records.

### Recommendations for JDBC Users

- If you care most about **job stability** and can accept a very small amount of data loss:
  - You can enable error handling with batching.
  - Monitor the error tables and logs.

- If you care most about **not losing any valid records**:
  - Consider keeping error handling disabled, or
  - Reduce `batch_size` (even to `1`) when enabling error handling, so that each batch contains at most one record.
  - Always test with your exact database and JDBC driver before enabling this in production.

## Multi-Table Sinks (Current State)

SeaTunnel has support for **multi-table sinks** (`MultiTableSinkWriter`) and provides
hooks so that row-level errors from per-table writers can be sent to the shared
error handler.

However, from a **user** point of view, you should treat multi-table + error
handling as **experimental and not fully supported** in this version:

- Only the JDBC writer defines clear behavior after a row-level error (clearing its batch).
- Other writers may keep internal buffers or transactions that have not been fully
  tested with row-level error handling.
- Cross-table consistency guarantees are **not** defined when some tables see
  row-level errors and others do not.

We recommend:

- Start using error handling in **single-table** sink jobs first.
- If you really need it in multi-table jobs, test your end-to-end flow thoroughly
  and be prepared to roll back or disable error handling if behavior is not as expected.

## Basic Configuration Example (Single-Table JDBC Sink)

Below is a minimal example that routes sink errors to a JDBC error table:

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"              # or LOG / DISABLE
    max_error_ratio = 0.01       # fail job if >1% of records are errors
    max_error_records = 1000     # or if more than 1000 error records
    queue_capacity = 10000
    queue_overflow_policy = "FAIL"  # FAIL / DROP / BLOCK

    include_original_data = true
    include_stacktrace = false
    original_data_format = "TEXT"
    original_data_max_length = 8192

    sink {
      plugin_name = "Jdbc"
      error_table = "orders_sink_error_basic"
      # configure Jdbc sink options for the error table here
    }
  }
}
```

For transform stages, you can configure `transform_error_handler` in a similar way.

## Configuration & Parameters

### Where to Configure

Error handling can be configured at two levels:

- **Stage level (env)** – for all transforms or all sinks in the job:
  - `env.transform_error_handler` – applies to the transform stage.
  - `env.sink_error_handler` – applies to the sink stage.
- **Global (env)** – defaults for both stages:
  - `env.error_handler` – global defaults when stage-level settings do not provide a value.

Field precedence (per parameter):

1. Stage-level `env.transform_error_handler` / `env.sink_error_handler` (highest).
2. Global `env.error_handler` (default `DISABLE`).

### Main Parameters

- `mode` (string)
  - What to do with row-level errors.
  - Values: `DISABLE` (default), `LOG`, `ROUTE`.
- Threshold counters (current behavior)
  - Thresholds are evaluated using internal counters:
    - `totalRecords`: incremented once per `write(...)` call in sink, and once per transform operator invocation (`map(...)` or `flatMap(...)`) in transform chains.
    - `errorRecords`: incremented once per row-level error handled.
  - For transform chains, counters are shared across all wrapped operators in the chain.
- `max_error_ratio` (double)
  - Maximum allowed ratio of error records (0.0–1.0).
  - Example: `0.01` means fail the job if more than 1% of processed records are errors.
  - Default: `0.0` (no ratio-based limit).
- `max_error_ratio_min_records` (int)
  - Warm-up threshold before `max_error_ratio` takes effect.
  - When total processed records is less than this value, ratio checks are skipped to avoid failing too early on tiny samples.
  - Default: `100`.
- `max_error_records` (long)
  - Maximum absolute number of error records allowed.
  - Default: `0` (no count-based limit).
- `queue_capacity` (int)
  - Internal buffer size for pending error records.
  - Default: `10000`.
- `queue_overflow_policy` (string)
  - What to do when the error queue is full.
  - Values:
    - `FAIL` (default) – fail the job.
    - `DROP` – drop new error records and keep the job running.
    - `BLOCK` – block the producer until space is available (may affect throughput).
- `include_original_data` (boolean)
  - Whether to include the original record in the error payload.
  - Default: `false`.
- `include_stacktrace` (boolean)
  - Whether to include the full Java stacktrace in the error payload.
  - Default: `false`.
- `original_data_format` (string)
  - Reserved parameter.
  - Current version only supports `TEXT` and stores `original_data` as the string representation of the record (`String.valueOf(row)`).
- `original_data_max_length` (int)
  - Maximum length of the serialized original data.
  - Records longer than this may be truncated.
  - Default: `8192`.

### Error Sink Parameters

Under `..._error_handler.sink` you define where error records go:

- `plugin_name` (string)
  - The connector used for the error sink, for example `Jdbc`.
- `error_table` (string, JDBC-specific)
  - Target table for error records (for example `orders_sink_error_basic`).
- Other connector-specific options
  - For JDBC, you must still configure `url`, `username`, `password`, `driver`, etc., the same way as a normal JDBC sink.

If you do not configure a `sink` block, `ROUTE` mode will still classify row-level errors but has nowhere to write them, so only logging will be available.

## Summary

- Error handling lets you keep jobs running when some records are bad, by
  logging or routing them to an error sink.
- The feature is **experimental** and **disabled by default**.
- In JDBC, a row-level error currently causes the **whole batch** in memory to
  be treated as error data, so some valid records in the same batch may be lost.
- Multi-table + error handling is experimental and not fully supported yet.
- Always validate behavior in your own environment before turning this on in production.
