---
sort: 2
---

# Complex example

In the following a more complex example based on nested references inside a `foreach` sink.

## Plan

Location: `http://server/plan.yaml`:
```yaml
components:
  input: { type: ref }
  tx: { type: ref, ref: transformation }
sinks: 
  ouput: { type: ref }
pipelines:
  pipe: { component: tx, sink: ouput }
```

## `input` Component

Location: `http://server/plan_components_input.yaml`:
```yaml
type: stream
  format: rate
```

## `tx` Component

Location: `http://server/plan_transformation.yaml`:
```yaml
sql: >
  select *, value * 100 as bigValue from input
```

## `output` Sink

Location: `http://server/plan_sinks_output.yaml`:
```yaml
type: foreach
  name: ouputQuery
  trigger: { milliseconds: 60 }
  batchComponentName: source
  plan:
    sinks:
      out: { type: ref }
    pipelines:
      out: { component: source, sink: out }
```

## `out` Sink inside Foreach

Location: `http://server/plan_sinks_output_plan_sinks_out.yaml`:
```yaml
type: batch
  format: parquet
  options: { path: 'hdfs://cluster/file.parquet' }
  mode: OVERWRITE
```
