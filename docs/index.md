# Welcome

## What is this?

This project aim is at simplifying the writing, maintenance and testing of complex [Apache Spark](https://spark.apache.org) pipelines (both in batch and streaming mode). 
While Spark is an optimal platform to express and implement data transformation and integration, teams collaborating on it may find it difficult to divide and organize work on different part of the pipeline.
Moreover, many times these pipelines are wired and declared in different way, adding complexity where none is needed.

## The Problem

Did you ever find yourself writing many (too many) datasets operations (map, flatmap, filter, join, etc etc) and then
making a big data pipeline with them? 
How about joining different datasets? 
How about maintaining datasets sources configurations?

Some of your operations are sql statements, others are dataset operations. 
Some are aggregations, some are joins or unions. 
Many engineers and teams may need to change alter test and modify pieces of a pipeline.
Multiple pipelines may even have different configurations.

Your software may be looking something like:
```scala
val df1 = spark.sql("select stuff from source1")
df1.createOrReplaceTempView("table1")

val df2 = sparl.sql("select even more stuff from source 2")
df2.createOrReplaceTempView("table2")

val df3 = spark.sql("select things from table1 join table2 on some id")
df3.createOrReplaceTempView("table3")

val df4 = df3.join(df1).on(some_condition)

\\ and so on ...
df10.write.save
```

You find out that organizing your code like this add complexity, because it is:

* ... hard to manage
* ... difficult to compose
* ... complex to debug
* ... impossible to maintain and test in isolation
* ... time-consuming to understand where a table is defined
* ... what about the other pipeline down there, that is completely different?
* ... and, do you even remember what the schema of a table is?

## The Solution

Having a **declarative way to describe the pieces of your pipeline** will provide an easier to manage structure, where every team or engineer can provide pieces for it, without interfering with other teams.
With this organization the complexity of the system can be kept in check, because:
* ... every pipeline is organized the same way
* ... each pipeline pieces can be tested in isolation
* ... each piece declares its inputs and has a deterministic outcome (same input will yield same output)
* ... there is a clear separation between the spark configuration (number of nodes, resources, execution environment) and what the pipeline is supposed to do
* ... code extensions are possible, and they are isolated by providing an external piece of code that will be executed by a well-defined part of the pipeline

So, for example, a pipeline might look like a yaml file like this:
```yaml
components:
  source1:
    type: batch
    format: parquet
    options: { path: hdfs://... }
  source2:
    type: ref
    ref: http://some/well/known/location.yaml
  source3:
    type: inline
    data:
      - { column1: "value1", column2: 10 }
      - { column1: "value2", column2: 20 }
    schema: "`column1` STRING,`column2` INT"
    
  transformation1:
    type: sql
    using: [ source1, source2 ]
    sql: select * from source1 union all source 2
  transformation2:
    type: transform
    using: [ transformation1, source3 ]
    transformWith: com.yourname.yourpackage.SomeTransformation

sinks:
  sink1:
    type: batch
    format: kafka
    options: { ... }

pipelines:
  samplePipeline: { component: transformation2, sink: sink1 }
```

Notice how:
* ... a pipeline is declared to be a combination between a component and a sink
* ... a component is used to create a dataset
* ... components (and sinks) can be declared as external (they are referenced as external resources in http, hdfs, or jar files)
* ... each component can generate a dataset, and with mock inputs, can be tested in isolation

Executing this plan is as easy as you might guess:
```shell
cd spark/bin
./spark-submit --master local --packages sparkengine:plan-app:x.x.x \
  --class sparkengine.plan.app.Start spark-internal -p myPlan.yaml
```

Without coding any `main` code you can start your plan by using a pre-defined startup facility that is responsible for:
* ... find all external references
* ... importing all external code (from a maven repository)   
* ... validating your plan
* ... monitor its execution
* ... release resources

The same plan can be executed, locally, remotely (in yarn, mesos or k8s), in zeppelin or jupyter, with different inputs for testing or for production.
The final goal? reduce complexity, have better development workflow, simplify and unify the pipeline development, isolate pieces, and make engineers happier :)

## What's next?

* Go to [quick start](/quick_start/up-and-running-in-5-minutes.html) to test the tires. (_Almost_) no software required.

