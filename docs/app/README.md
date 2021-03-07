---
layout: default
title: Application
nav_order: 6
has_children: false
permalink: /app
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
./spark-submit --master local --packages com.spark-engine:spark-app:1.0 
    --class sparkengine.plan.app.Start spark-internal 
    -p file:///tmp/plan.yaml
```

## Command line help

Usage:

```text
Usage: <main class> [options]
  Options:
    -h, --help
      Help usage
  * -p, --planLocation
      Location of the execution plan (in yaml format)
    -s, --sqlResolution
      For sql components, provide validation and/or dependency discovery
      Default: VALIDATE
      Possible Values: [SKIP, VALIDATE, INFER]
    --skipRun
      Do everything, but do not run the pipelines
      Default: false
    --pipelines
      Provide an list of pipelines to execute (if pipeline is not in plan, it 
      will be ignored)
    -l, --log
      Set main application log level (one of 
      OFF,FATAL,ERROR,WARN,INFO,DEBUG,TRACE,ALL) 
      Default: INFO
    --parallelPipelineExecution
      Executes the pipelines of the plan in parallel (instead of sequentially)
      Default: false
    --skipFaultyPipelines
      Skip a faulty pipeline (instead of exiting the application)
      Default: false
    --skipResolution
      Skip any resolution of the plan (plan will be executed as-is!)
      Default: false
    --skipStackTrace
      Skip full stackTrace
      Default: false
    --writeResolvedPlan
      write the resolved plan (to a temporary location)
      Default: false
```
