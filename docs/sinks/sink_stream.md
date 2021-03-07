---
layout: default
parent: Sinks
nav_order: 6
---

# Stream Sink

This sink describes a write operation of a streaming dataset to the standard spark writer interface.
Refer to spark documentation for details.

## Fields

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

Yaml Example:
```yaml
collectSink:
  type: stream
  name: queryNameHere
  format: kafka
  options: { path: 'abc' }
  trigger: { milliseconds: 60 }
  mode: APPEND
```
