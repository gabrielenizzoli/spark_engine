---
sort: 1
---

## Execution Plan

As we stated before, an _execution plan_ is a set of datasets that can be routed to a set of consumers.
A single pair dataset/dataset consumer is called a _pipeline_, while the full set of all components/consumers/pipelines is an _execution plan_.
The model class that represents an execution plan is `sparkengine.plan.model.pipeline.Plan`.

In yaml term, an execution plan can be represented by a document divided in 3 parts:
* a list of _components_ - this will describe datasets
* a list of _sinks_ - this will define dataset consumers
* a list of _pipelines_ - to pair a component with a sink

### Examples

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
  - { component: component1, sink: consumer1 }
  - { component: component2, sink: consumer2 }
```

