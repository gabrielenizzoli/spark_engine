---
sort: 1
---

# What is a Component

This project is based on a model abstraction to describe a dataset. 
The final goal of this model is to be able to describe how a spark dataset can be composed.
Since in spark a dataset is defined as a set of operations on data, a component encapsulate a logical subset of these operations.
For example, we might be interested in acquiring data from a source, modify it to fit a given schema, aggregate it so that we can compute some statistics, join it with a different dataset, and finally save the outcome somewhere.
All these steps can be separated and described as standalone operations that can be tested in isolation, and later composed to create a complete end-to-end workflow.

An additional advantage to have a model describing a set of transformations is that it can be expressed in a shareable format (usually yaml or json).

In the spark engine project, these parts that describe a dataset are called **components**.

## Dataset Components

A component is an abstraction that describes how a dataset is built. 
In Java terms, the root of the component hierarchy is the `sparkengine.plan.model.component.Component` interface. Every component extends form it.

The common characteristic of a component is to carry all the data needed to describe how to produce or transform a dataset.
Some components provide information on how to generate datasets from external sources (like the _batch_ component), while others make use of an existing dataset and transforms it (like the _encode_ component).
By properly chaining many components it is thus possible to compose a complex dataset.

In an execution plan many components work together to provide information about how to build a named datasets. 
This is possible because in an execution plan each component is associated to a unique name.

In the following sample yaml execution plan a `operation` component uses as an input the dataset provided by two other components:
```yaml
source1:
  type: inline
  data:
    - { column1: "value1", column2: 10 }
    - { column1: "value2", column2: 20 }

source2:
  type: inline
  data:
    - { column1: "value3", column2: 10 }
    - { column1: "value4", column2: 30 }

operation:
  type: sql
  using: [ source1, source2 ]
  sql: select * from source1 join source2 on source1.column2 = source2.column2
```

An execution plan is _consistent_ if all the component referenced are available and if there are no circular references (ie: the graph of components must be an acyclic graph).
By referencing a component name in a consistent execution plan, all the information to generate a dataset is available.
In the example above, if the plan is consistent, the `operation` components should carry all the information required to generate a dataset, once the datasets from the component `source1` and `source2` are generated.

The practical advantages of splitting a complex system into smaller sets are:
* _easier to develop_ - develop a ful plan, but only focus on a step at a time
* _clear inputs and outputs_ - each component is fully described by the model, and it is not possible to reference datasets not explicitly defined in the model
* _easier to test in isolation_ - test each component independently, by providing mock input data and then verifying the output
* _easier to refactor_ - changes can be planned and implemented in stages

