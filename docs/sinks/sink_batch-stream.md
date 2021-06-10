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
