---
sort: 5
---

### Batch Sink

This sink describes a write operation of a dataset to the standard spark writer interface.
Refer to spark documentation for details.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `batch` |
| `format` | yes | Spark writer format (eg: `parquet`, `json`). |
| `mode` | no | Write mode. One of: `APPEND`, `OVERWRITE`, `ERROR_IF_EXISTS`, `IGNORE`. |
| `options` | no | A key-value map of options. Meaning is format-dependent |
| `partitionColumns` | no | A list of columns to be used to partition during save of data. |

Yaml Example:
```yaml
collectSink:
  type: batch
  format: parquet
  options: { path: 'abc' }
  mode: OVERWRITE
```
