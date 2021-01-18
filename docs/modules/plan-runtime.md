---
sort: 2
---

# The pipeline-runtime module

The main 3 abstractions provided are the `DatasetConsumer`, the `DatasetFactory` and the `Pipeline`.
The _factory_ is responsible for creating a new dataset given a dataset name. The dataset is implementation dependent.
The _consumer_ is acting upon a dataset and doing something with it, like saving to the file system, showing the output to terminal or starting a stream.
Finally, the _pipeline_ is composing these two items and providing an easy way to execute a full end-to-end pipeline.

Example of a dataset factory:
```java
var datasets = Map.of("data", SparkSession.active().reader().parquet("hdfs://parquet/file/to/read"));
DatasetFactory datasetFactory = (name) -> Optional.ofNullable(datasets.get(name)).orElseThrow(...);

var ds = datasetFactory.buildDataset("data");
```

Example of a dataset consumer factory:
```java
var ds = ...;

var consumers = Map.of("show", ShowConsumer.builder().build());
DatasetConsumerFactory consumerFactory = (name) -> Optional.ofNullable(consumers.get(name)).orElseThrow(...);

factory.buildConsumer("show").readFrom(ds);
```

And a pipeline:
```java
var datasetFactory = ...;
var consumerFactory = ...;
var pipelineFactory = PipelineFactory.builder()
        .datasetFactory(datasetFactory)
        .datasetConsumerFactory(consumerFactory)
        .build();

pipelineFactory.buildPipeline("dataset", "show").run();
```

Obviously the benefit will be to have a proper way to quickly define datasets. 
We will see in a later module that a properly configured `DatasetFactory` wil be able to compose very complicate execution plans!