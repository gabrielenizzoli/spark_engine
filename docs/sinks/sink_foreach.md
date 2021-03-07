---
layout: default
parent: Sinks
nav_order: 7
---

# Foreach Sink

A streaming dataset can write its output in batch mode.
This sink will wire each micro batch dataset to one or more pipelines described as a full fledged execution plan (see below).
Note that the execution plan will be fully reevaluated on every new micro batch dataset (ie: every trigger).

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `foreach` |
| `name` | yes | Name of the streaming query. |
| `mode` | no | Write mode. One of: `APPEND`, `COMPLETE`, `UPDATE`. |
| `trigger` | no | Defines trigger for stream. |
| `options` | no | A key-value map of options. Meaning is format-dependent |
| `plan` | yes | An execution plan that will describe all the operations that should be executed on the micro batch dataset  |
| `batchComponentName` | yes | The name of the virtual component that can be referenced in the plan. This component will provide a dataset equal to the micro batch of the stream |

## Examples

Yaml Examples:

```yaml
foreachSink:
  type: foreach
  name: queryName
  trigger: { milliseconds: 1000 }
  mode: APPEND
  batchComponentName: src
  plan:
    components:
      transformation1: { type: sql, using: src, sql: "select column from src" }
      transformation2: { type: encode, using: transformation1, encodedAs: { type: INT } }
    sinks:
      display: { type: show }
      fileOutput: { type: batch, format: parquet, options: { path: 'hdfs://...' }, mode: OVERWRITE }
    pipelines:
      - { component: src, sink: display }
      - { component: transformation2, sink: fileOutput }
```
