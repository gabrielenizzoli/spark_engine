---
layout: default
title: Home
nav_order: 1
description: "Spark Engine is here"
permalink: /
---

# Focus on the data, not the boring parts
{: .no_toc }

Spark Engine let you focus on writing effective data pipelines. Everything else (all the scaffolding, application wiring, metrics collection, validation, source and sink definitions, etc) is managed.
{: .fs-6 .fw-300 }

[Get started now](/quickstart){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 } [View it on GitHub](https://github.com/gabrielenizzoli/spark_engine){: .btn .fs-5 .mb-4 .mb-md-0 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## What is Spark Engine?

This project aim is at simplifying the writing, maintenance and testing of complex [Apache Spark](https://spark.apache.org) pipelines (both in batch and streaming mode).
Spark is an optimal platform to express and implement data transformation and integration, and this projects makes them declarative, not programmatic.

## Make it fast and easy to write Spark Pipelines

Spark Engine provides a **declarative way to describe the pieces of the data pipeline**.
A n execution plan declared this way will be easy to manage because:

* ... every pipeline in the plan is structured in the same way;
* ... each pipeline piece can be tested in isolation;
* ... each piece declares its inputs, provides an output, and has a deterministic outcome;
* ... there is a clear separation between the spark configuration (number of nodes, resources, execution environment) and what the pipeline is supposed to do;
* ... extensions are possible, and they are isolated as external packages.

So, for example, a pipeline might look like a yaml file:

```yaml
components:
  sourceWithData:
    type: batch
    format: parquet
    options: { path: hdfs://... }
  sourceDefinedSomewhereElse:
    type: ref
    ref: http://some/well/known/location.yaml
  sourceWithInlineData:
    type: inline
    data:
      - { column1: "value1", column2: 10 }
      - { column1: "value2", column2: 20 }
    schema: "`column1` STRING,`column2` INT"
  unionDifferentSources:
    sql: select * from sourceWithData union all sourceDefinedSomewhereElse
  transformDataWithCustomCode:
    type: transform
    using: [ unionDifferentSources, sourceWithInlineData ]
    transformWith: com.yourname.yourpackage.SomeTransformation

sinks:
  sendYourDataToKafka:
    type: batch
    format: kafka
    options: { ... }

pipelines:
  yourPipelineNameHere: { component: transformDataWithCustomCode, sink: sendYourDataToKafka }
```

Notice how:

* ... a pipeline is declared to be a combination between a component and a sink
* ... a component is used to declare how a dataset is built
* ... components (and sinks) definitions can be external (they are referenced as external resources in http, hdfs, or jar files)
* ... each component has well defined inputs and outputs, and, with mock inputs, can be tested in isolation

Executing this plan is [trivial](/app/command_line):

```shell
cd spark/bin
./spark-submit --master local --packages com.spark-engine:plan-app:x.x.x,com.yourname:yourpackage:x.x.x \
  --class sparkengine.plan.app.Start spark-internal -p myPlan.yaml
```

A plan is started by a pre-defined startup facility (the `sparkengine.plan.app.Start` class) that is responsible for:

* ... finding all external references
* ... importing all external code (from a maven repository)
* ... validating the plan
* ... exporting all accumulators as metrics to graphite/prometheus/etc etc
* ... monitoring its execution
* ... releasing resources

[An embedded option is also available](/app/embedded), so a plan can be executed in any custom code.
The same plan can be executed, locally, remotely (in yarn, mesos or k8s), in zeppelin or jupyter, with different inputs, for testing or for production.
The final goal? reduce complexity, have better development workflow, simplify and unify the pipeline development, isolate pieces, and make engineers happier :)

## What's next?

* Go to [quick start](/quickstart) to test the tires. (_Almost_) no software required.
