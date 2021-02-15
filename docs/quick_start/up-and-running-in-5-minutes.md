---
sort: 1
---

# Get up and running in 5 minutes (or less)

## Get Java + Spark

* Download and unpack [Apache Spark, version 3+](https://spark.apache.org/downloads.html).
* Also, you should also have [Java 11+ installed](https://aws.amazon.com/corretto/).

## Use an execution plan

An execution plan describes one or more spark data pipelines, streams or batches.
Usually an execution plan is hosted in hdfs and witten in yaml.
The plan we will use is available directly from the GitHub repository here: [quickStartPlan.yaml](https://raw.githubusercontent.com/gabrielenizzoli/spark_engine/master/examples/plans/quickStartPlan.yaml).
This sample plan has 2 pipelines, one is a batch pipeline, the other is a stream:

### quickStartPLan.yaml

The plan looks like this:
```yaml
components:
  sqlSource: { type: ref }
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
  batch: { component: sqlSource, sink: showTable }
  stream: { component: sqlOnRate, sink: showRate }
```

And the referenced component:
```yaml
sql: >
  select 'value' as column
```

## Run the execution plan

Regarding Spark, in the `bin/` sub-folder there is the `spark-submit` command.
Running our sample remote plan it is as easy as executing:
```shell
cd spark/bin
./spark-submit --master local --packages sparkengine:plan-app:x.x.x --class sparkengine.plan.app.Start spark-internal -p https://raw.githubusercontent.com/gabrielenizzoli/spark_engine/master/examples/plans/quickStartPlan.yaml
```

**That is it!** Note how this plan will execute both the batch operation and the stream.
Since the stream is open-ended, the application will not stop, but it will run forever.

## What's next

This is all the basic stuff that you need to know on how to run an execution plan.
Notice how this library does not interfere at all with the spark configuration, which can be passed, as usual, to the `spark-submit` command. 
Also note that this plan can run locally (as in this quick example) and also in kubernetes / mesos / yarn / spark standalone / _etc etc_.
