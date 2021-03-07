---
layout: default
title: Execution Plan
nav_order: 5
has_children: false
permalink: /plan
---

# Execution Plan

As we stated before, an _execution plan_ is a set of datasets that can be routed to a set of consumers.
A single pair dataset/dataset consumer is called a _pipeline_, while the full set of all components/consumers/pipelines is an _execution plan_.
The model class that represents an execution plan is `sparkengine.plan.model.pipeline.Plan`.

In yaml term, an execution plan can be represented by a document divided in 3 parts:

* a list of _components_ - this will describe datasets
* a list of _sinks_ - this will define dataset consumers
* a list of _pipelines_ - to pair a component with a sink

## Fields

| Field | Possible Value |
| ----- | -------------- |
| `components` | A set of named components. |
| `sinks` | A set of named sinks.  |
| `pipelines` | A list of pipelines, where each pipeline names a component to provide a dataset and a sink to consume the dataset. The pipeline is a yaml object with a `component` field and a `sink` field. |

## Examples

A high level example in yaml:

```yaml
components:
  source1:
    ...
  source2:
    ...
  component1:
    using: [ source1, source2 ]
    ...    
  component2:
    using: [ component1 ]
    ...

sinks:
  consumer1:
    ...
  consumer2:
    ...

pipelines:
  pipe1: { component: component1, sink: consumer1 }
  pipe2: { component: component2, sink: consumer2 }
```

A practical example:

```yaml
components:
  sql: { sql: "select 'value' as column" }
  rate: { type: stream, format: rate }
  sqlOnRate: { using: [rate], sql: "select *, value * 100 as bigValue from rate" }

sinks:
  showTable: { type: show }
  showRate: 
    type: stream
    name: query
    format: console
    mode: APPEND
    trigger: { milliseconds: 1000 }

pipelines:
  batch: { component: sql, sink: showTable }
  stream: { component: sqlOnRate, sink: showRate }
```

Notes:

* a sink or component that is not used in a pipeline will simply not be utilized,
* a pipeline with the same component and sink can be repeated multiple times,
* a plan with no pipelines will do nothing,
* pipelines are executed in non-deterministic order,
* a missing sink or component will cause the pipeline (and eventually the plan) to fail.
