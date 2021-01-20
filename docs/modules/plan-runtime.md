---
sort: 4
---

# The plan-runtime module

The main 3 abstractions provided are the `DatasetConsumer`, the `Dataset` and the `PipelineRunner`.
The `Dataset` is the main spark unit of data, and it represents a logical table. 
A `DatasetFactory` is responsible for creating a new dataset given a dataset name.
The `DatasetConsumer` is acting upon a dataset and doing something with it, like saving to the file system, showing the output to terminal or starting a stream. 
A `DatasetConsumerFactory` is the means by which dataset consumers are created.
Finally, the `PipelineRunner` is composing these two items and providing an easy way to execute a full end-to-end pipeline. 
Even here a factory object, a `PlanFactory`, will be in charge of creating the runner.

## Dataset Examples

Example of a dataset factory:
```java
var datasets = Map.of("data", sparkSession.reader().parquet("hdfs://parquet/file/to/read"));
DatasetFactory datasetFactory = (name) -> Optional.ofNullable(datasets.get(name)).orElseThrow(...);

var ds = datasetFactory.buildDataset("data");
```

For convenience, a static builder from a map is available:
```java
var datasets = Map.of("data", sparkSession.reader().parquet("hdfs://parquet/file/to/read"));
var datasetFactory = DatasetFactory.ofMap(datasets);
```

## DatasetConsumer Example

Example of a dataset consumer factory:
```java
var ds = ...;

var consumers = Map.of("show", ShowConsumer.builder().build());
DatasetConsumerFactory consumerFactory = (name) -> Optional.ofNullable(consumers.get(name)).orElseThrow(...);

factory.buildConsumer("show").readFrom(ds);
```

For convenience, a static builder from a map is available:
```java
var consumers = Map.of("show", ShowConsumer.builder().build());
var consumerFactory = DatasetConsumerFactory.ofMap(datasets);
```

## PipelineRunner Example

Each pipeline in an execution plan is defined by the name of the dataset and a consumer.

A `PipelineName` is created by providing the two names:
```java
var pipelineName = PipelineName.of("dataset", "datasetConsumer");
```

And a pipeline:
```java
var pipelineName = PipelineName.of("dataset", "consumer");

var pipelineNames = List.of(pipelineName);
var datasetFactory = ...;
var consumerFactory = ...;
var planFactory = SimplePlanFactory.builder()
        .pipleineNames(pipleineNames)
        .datasetFactory(datasetFactory)
        .datasetConsumerFactory(consumerFactory)
        .build();

var pipelineRunner = planFactory.buildPipelineRunner(pipelineName);
pipelineRunner.run();
```

Obviously the benefit will be to have a proper way to quickly define datasets. 
We will see in a later module that a properly configured `DatasetFactory` wil be able to compose very complicate execution plans!