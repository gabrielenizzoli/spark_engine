---
sort: 5
---

## Batch and Stream Component

In spark, it is possible to define an external source (like a file, or a kafka topic).
This component allows for defining one of such sources.

### Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | One of: `batch`, `stream` |
| `format` | yes | One of the supported values supported by spark natively, lik e`parquet` or `json` |
| `options` | no | A map of values |
| `encodedAs` | no | An optional encoded specification |

### Examples

Yaml examples:
```yaml
# parquet
parquetSource:
  type: batch
  format: parquet
  options:
    path: hdfs://...
    
# csv
csvSource:
  type: batch
  format: com.databricks.spark.csv
  options:
    inferSchema: true
    path: hdfs://...
```

Note that if a dataset need to be of streaming variety, then the type need to be `stream`.
All the other fields are the same.

Yaml examples:
```yaml
# kafka
parquetSource:
  type: stream
  format: kafka
  options: { "kafka.bootstrap.servers": host1:port1,host2:port2, subscribe: topic1 }
```
