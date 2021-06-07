---
layout: default
title: Command Line
parent: Application
nav_order: 1
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
./spark-submit --master local --packages com.spark-engine:plan-app:x.x.x
    --class sparkengine.plan.app.Start spark-internal 
    -p file:///tmp/plan.yaml
```

## Run in docker

This will also work in docker: just use a prebuilt spark image.
If you have a standard spark image, all you need to do is just run your `spark-submit` command.
Here is a sample:

```shell
docker run \
  --rm datamechanics/spark:jvm-only-3.1.1-latest /opt/spark/bin/spark-submit \
  --master local --packages com.spark-engine:plan-app:0.13.0 --class sparkengine.plan.app.Start spark-internal \
  -p https://raw.githubusercontent.com/gabrielenizzoli/spark_engine/master/examples/plans/quickStartPlan.yaml
```

## More complicated examples

More complex examples (like a wikipedia downloader) may be found in the [GitHub repository](https://github.com/gabrielenizzoli/spark_engine/tree/master/examples).

## Parameters

The inline component may be configured by providing a parameter map using the command line (using the `-P` option).
The parameter map will be used to replace values in the inline dataset of the component.
If `--parametersFromEnvironment` is also specified, all the environment variables will also be imported in the parameter map as well.

## Command line help

Usage:

```text
Usage: <main class> [options]
  Options:
    -p, --plan
      The location of the plan. If missing, the source will be the standard 
      input. 
    -h, --help
      Help usage
    -s, --sqlResolution
      For sql components, provide validation and/or dependency discovery
      Default: VALIDATE
      Possible Values: [SKIP, VALIDATE, INFER]
    --pipelines
      Provide a subset of pipelines to execute (if pipeline name provided is 
      not in plan, it will be ignored)
    --skipRun
      Do everything, but do not run the pipelines
      Default: false
    -l, --log
      Set main application log level (one of 
      OFF,FATAL,ERROR,WARN,INFO,DEBUG,TRACE,ALL) 
      Default: INFO
    --parallelPipelineExecution
      Executes the pipelines of the plan in parallel (instead of sequentially)
      Default: false
    -P, --parameter
      <key>=<value> list of zero or more parameters to be replaced in an 
      inline component dataset
      Syntax: -Pkey=value
      Default: {}
    --parametersFromEnvironment
      Add to the parameter list any environment variable
      Default: false
    --skipFaultyPipelines
      Skip a faulty pipeline (instead of exiting the application)
      Default: false
    --skipResolution
      Skip any resolution of the plan (plan will be executed as-is!)
      Default: false
    --skipStackTrace
      Skip full stackTrace when printing application errors
      Default: false
    --sparkSessionReuse
      Reuse spark session if already defined
      Default: false
    --writeResolvedPlanToFile
      Write the resolved plan to the specified plan
    --writeResolvedPlanToStdout
      Write the resolved plan to standard output
      Default: false
```
