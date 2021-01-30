---
sort: 1
---

# Get up and running in a few minutes

## 1. Create an execution plan

An execution plan describes one or more spark data pipelines, streams or batches.
Usually an execution plan is hosted in hdfs and witten in yaml.

Here is a sample execution plan with 2 pipelines, one is a batch pipeline, the other is a stream:
```yaml
components:
  sql: { type: sql, sql: select 'value' as column }
  rate: { type: stream, format: rate }
  sqlOnRate: { type: sql, using: [rate], sql: "select *, value * 100 as bigValue from rate" }

sinks:
  showTable: { type: show }
  showRate: 
    type: stream
    name: query
    format: console
    mode: APPEND
    trigger: { milliseconds: 1000 }
    options: 
      "spark.sql.streaming.forceDeleteTempCheckpointLocation": true

pipelines:
  - { component: sql, sink: showTable }
  - { component: sqlOnRate, sink: showRate }
```

## 2. Get spark

Download and unpack apache spark. In the `bin\` sub-folder there is the `spark-submit` command. 
Also, you should also have Java installed.

## 3. Run the plan

If the plan is saved in the filesystem, in the `/tmp` folder, with the `plan.yaml` name, then running it is as easy as executing:
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
