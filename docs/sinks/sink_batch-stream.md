---
layout: default
parent: Sinks
nav_order: 5
---

# Batch and Stream Sinks

This sink describes a write operation of a batch or streaming dataset to the standard spark writer interface.

Refer to spark documentation for details.

## Fields for the Batch Sink

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `batch` |
| `format` | yes | Spark writer format (eg: `parquet`, `json`). |
| `mode` | no | Write mode. One of: `APPEND`, `OVERWRITE`, `ERROR_IF_EXISTS`, `IGNORE`. |
| `options` | no | A key-value map of options. Meaning is format-dependent |
| `partitionColumns` | no | A list of columns to be used to partition during save of data. |

## Fields for the Stream Sink

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `batch` |
| `format` | yes | Spark writer format (eg: `kafka`). |
| `name` | yes | Name of the streaming query. |
| `mode` | no | Write mode. One of: `APPEND`, `COMPLETE`, `UPDATE`. |
| `checkpointLocation` | no | Checkpoint location for the stream (usually somewhere in hdfs or local filesystem). |
| `trigger` | no | Defines trigger for stream. |
| `options` | no | A key-value map of options. Meaning is format-dependent |

## Parameters

An batch or stream data sink can have some of its options replaced by command-line parameters or environment variables.
An option line is treated as a template where the replacements are enclosed in the `${` and `}` brackets.
If a replacement is not present and no default value is provided, an error will be thrown.
Parameter can be passed on the command line at start-time and can be optionally read from the environment.

In its more generic form, a parameter replacement looks like `${name:default value}`, where:

* `name` is the expected parameter name to use to replace in the component;
* `default value` is an optional non-null or non-empty value to use if a parameter is not provided.  

As an example, in the following component the `${n:NAME:100}` string will be replaced by the value of the `NAME` parameter, after converting it to a number.

## Examples

Yaml Example for a Batch Sink:

```yaml
collectSink:
  type: batch
  format: parquet
  options: { path: 'abc' }
  mode: OVERWRITE
```

Yaml Example for a Stream Sink:

```yaml
collectSink:
  type: stream
  name: queryNameHere
  format: kafka
  options: { path: 'abc' }
  trigger: { milliseconds: 60 }
  mode: APPEND
```
