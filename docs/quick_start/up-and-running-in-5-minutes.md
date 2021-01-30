---
sort: 1
---

# Get up and running in a few minutes

## Create an execution plan

An execution plan describes one or more spark data pipelines, streams or batches.
Usually an execution plan is hosted in hdfs and witten in yaml.

Here is a sample execution plan with 2 pipelines, one is a batch pipeline, the other is a stream:
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
  - { component: sql, sink: showTable }
  - { component: sqlOnRate, sink: showRate }
```

## Get Java + Spark

* Download and unpack apache spark.
* Also, you should also have Java 11+ installed.

## Run the plan

Regarding spark, in the `bin/` sub-folder there is the `spark-submit` command.
Assuming the plan is saved in the filesystem, as `/tmp/plan.yaml`, then running it is as easy as executing:
```shell
cd spark/bin
./spark-submit --master local --packages sparkengine:spark-app:1.0 
    --class sparkengine.plan.app.Start spark-internal 
    -p file:///tmp/plan.yaml
```

That is it! Note how this plan will first execute the batch operation, then the stream operation.
Since the stream is open-ended, the application will not stop, but it will run forever.

## What's next

This is all the basic stuff that you need to know to run an execution plan.
Notice how this library does not interfere at all with the spark configuration, which can be passed, as usual, to the spark-submit command.
