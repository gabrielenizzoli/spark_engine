---
layout: default
parent: Components
nav_order: 5
---

# Batch and Stream Component

In spark, it is possible to define an external source (like a file, or a kafka topic).
This component allows for defining one of such sources.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | One of: `batch`, `stream` |
| `format` | yes | One of the supported values supported by spark natively, lik e`parquet` or `json` |
| `options` | no | A map of values |
| `encodedAs` | no | An optional encoded specification |

## Parameters

An data source can have some of its options replaced by command-line parameters or environment variables.
An option line is treated as a template where the replacements are enclosed in the `${` and `}` brackets.
If a replacement is not present and no default value is provided, an error will be thrown.
Parameter can be passed on the command line at start-time and can be optionally read from the environment.

In its more generic form, a parameter replacement looks like `${name:default value}`, where:

* `name` is the expected parameter name to use to replace in the component;
* `default value` is an optional non-null or non-empty value to use if a parameter is not provided.  

As an example, in the following component the `${n:NAME:100}` string will be replaced by the value of the `NAME` parameter, after converting it to a number.

## Examples

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
