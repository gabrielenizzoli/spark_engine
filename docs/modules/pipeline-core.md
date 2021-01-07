---
sort: 2
---

# The pipeline-core module

The basic building pieces of a pipeline are exposed in this module. They are the `DatasetConsumer` and
the `DatasetSupplier`. The first one is responsible to do something with a Dataset, while the supplier is the one that
provides a dataset.

## DatasetConsumer

A consumer is a class that does something with a dataset, usually an action that enforces the execution of the logical
plan. An example of a typical consumer action is a `write` to a destination storage or a kafka topic, a call to `show`,
a call to `collect` data in the driver, and so on. A `DatasetConsumer<T>` is simply an interface that extends
a `Consumer<Dataset<T>>`.

## DatasetSupplier

A `DatasetSupplier` supplies a `Dataset` using the call of the `get()` method. As an example:

```java
import dataengine.pipeline.core.supplier.DatasetSupplier;

DatasetSupplier<String> datasetSupplier=()->SparkSession.active().createDataset(Arrays.asList("a","b","c"),Encoders.STRING());
        var ds=datasetSupplier.get();
        var list=ds.collectAsList();

        Assertions.assertEquals(Arrays.asList("a","b","c"),list);
```

There are already available many kinds of predefined suppliers to create datasets in different ways. Example:

```java
import dataengine.pipeline.core.supplier.impl.*;

// list of objects
var supplier=InlineObjectSupplier.<Integer>builder()
        .objects(List.of(1,2,3,4))
        .encoder(Encoders.INT()).build();
        var ds=supplier.get();
        Assertions.assertEquals(List.of(1,2,3,4),ds.collectAsList());

// parquet
        var parquetSupplier=SourceSupplier.<Row>builder().batch().parquet("hdfs://...").build();
        var dsFromParquet=parquetSupplier.get();

// stream
        var streamSupplier=SourceSupplier.<Row>builder()
        .stream().format("rate").option("rowsPerSecond ","1")
        .build();
        var dsStream=streamSupplier.get();
```

An important characteristic is that dataset suppliers can also be composed!
For example, a supplier may be defined to depend on other suppliers. This way any input , and may also have a dataset
transformation. Example:

```java
// given
var supplier1=InlineObjectSupplier.<Integer>builder()
        .objects(List.of(1,2,3,5))
        .encoder(Encoders.INT()).build();

        var supplier2=InlineObjectSupplier.<Integer>builder()
        .objects(List.of(3,4,5))
        .encoder(Encoders.INT()).build();

        DataTransformation2<Integer, Integer, Row> join=Transformations
        .sql("source1","source2",
        "select source1.value from source1 join source2 on source1.value = source2.value");

// when
        var joinSupplier=DatasetSupplier2.<Integer, Integer, Row>builder()
        .parentDatasetSupplier1(supplier1)
        .parentDatasetSupplier2(supplier2)
        .transformation(join)
        .build();

// then
        Assertions.assertEquals(List.of(3,5),joinSupplier.get().select("value").as(Encoders.INT()).collectAsList());
```

## TODO

Datasets are usually lazy built by suppliers; this implies that until the `get()` method in the supplier is invoked, the
Dataset is not instantiated.

Suppliers can be composed to create complex graphs of suppliers. If for instance we have 2 different data sources
defined, we can merge them with a suppliers that wraps a refernce to th eoriginal 2 data sources and a transformations.

