---
sort: 6
---

# The command line application

At the end of the day, what we want is to have an easy way to run our execution plan inside a spark application, without having to do any complex coding.

## Running a plan

If our execution plan is somewhere in a folder in a hdfs-compatible filesystem (eg: in a location like `file:///tmp/plan.yaml`) and looks like this:
```yaml
components:
  sql: { type: sql, sql: select 'value' as column }

sinks:
  show: { type: show }

pipelines:
  batch: { source: sql, sink: show }
```

Then we can run it in a terminal:
```shell
cd spark/bin
./spark-submit --master local --packages sparkengine:spark-app:1.0 
    --class sparkengine.plan.app.Start spark-internal 
    -p file:///tmp/plan.yaml
```

## Command line help

Usage:
```dtd
Usage: sparkengine.plan.app.Start [options]
  Options:
    -h, --help
      Help usage
    -l, --log
      Set main application log level (one of 
      OFF,FATAL,ERROR,WARN,INFO,DEBUG,TRACE,ALL) 
      Default: INFO
  * -p, -planLocation
      Location of the execution plan (in yaml format)
    -skipFaultyPipelines
      Skip a faulty pipeline (instead of exiting the application)
      Default: false
    -parallelPipelineExecution
      Executes the pipelines of the plan in parallel (instead of sequentially)
      Default: false
```