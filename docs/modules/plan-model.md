---
sort: 3
---

# The pipeline-model module

This module exposes a model abstraction to describe a dataset. 
The final goal of the model is to be able to describe how a spark dataset can be built.
Moreover, the model can be expressed in a shareable format, usually yaml (or json), that the code is able to translate in a set of Java beans.

Once a dataset is ready, it needs to be consumed to do something.
A consumer describes an action on a dataset (ie: save to disk, show to terminal, etc etc).
For this reason, a model abstraction is also provided to describe dataset consumers.

## Utilities

Some components in the model share some common abstractions. These abstractions are discussed here.

### Encoder

Datasets in spark can be encoded (something similar to a strong typing). 
Different part sof the model usually define an encoder. If present, the encoder will describe what format should be applied to the dataset. 

Available encoders are `value`, `tuple`, `bean` and `seralization`:

| Encoder Type | Possible Value |
| ------------ | -------------- |
| `value` | One of: `BINARY`, `BOOLEAN`, `BYTE`, `DATE`, `DECIMAL`, `DOUBLE`, `FLOAT`, `INSTANT`, `INT`, `LOCALDATE`, `LONG`, `SHORT`, `STRING`, `TIMESTAMP` |
| `tuple` | A list of 2 or more encoders |
| `bean` | The fully qualified name of a a Java class to represent the schema and type of all fields |
| `serialization` | One of: `JAVA`, `KRYO`; plus a class name. |

Some example of an encoder in yaml representation:
```yaml
# value
{ schema: value, value: STRING }

# tuple
{ schema: tuple, of: [ { schema: value, value: STRING }, { schema: value, value: INT } ] }

# bean
{ schema: bean, ofClass: some.java.class.Bean }

# serialization
{ schema: serialization, variant: KRYO , ofClass: some.java.class.Bean }
```

Note that the default serialization is `value`, so it can be omitted:
```yaml
# same
{ value: STRING }
{ schema: value, value: STRING }

# same
{ schema: tuple, of: [ { value: STRING }, { value: INT } ] }
{ schema: tuple, of: [ { schema: value, value: STRING }, { schema: value, value: INT } ] }
```

## Dataset Components

A component is an abstraction that describes how a dataset is built. 
In Java terms, the root of the component hierarchy is the `sparkengine.plan.model.component.Component` interface. Every component extends form it.
The common characteristic of a component is to carry all the data needed to produce or transform a dataset described by another component.
Some components provide information on how to generate datasets from external sources (like the _batch_ component), while others make use of an existing dataset and transforms it (like the _encode_ component).
By properly chaining many components it is thus possible to compose a complex dataset.

In an execution plan many components work together to provide information about how to build datasets.
In an execution plan each component is associated to a unique name. 

In the following sample yaml execution plan a `operation` component uses as an input the dataset provided by two other components:
```yaml
source1:
  type: inline
  data:
    - { column1: "value1", column2: 10 }
    - { column1: "value2", column2: 20 }

source2:
  type: inline
  data:
    - { column1: "value3", column2: 10 }
    - { column1: "value4", column2: 30 }

operation:
  type: sql
  using: [ source1, source2 ]
  sql: select * from source1 join source2 on source1.column2 = source2.column2
```

An execution plan is _consistent_ if all the component referenced are available and if there are no circular references (ie: the graph of components must be an acyclic graph).
By referencing a component name in a consistent execution plan, all the information to generate a dataset is available.
In the example above, if the plan is consistent, the `operation` components should carry all the information required to generate a dataset, once the datasets from the component `source1` and `source2` are generated.

The practical advantages of splitting a complex system into smaller sets are:
* _easier to develop_ - develop a ful plan, but only focus on a step at a time
* _clear inputs and outputs_ - each component is fully described by the model, and it is not possible to reference datasets not explicitly defined in the model
* _easier to test in isolation_ - test each component independently, by providing mock input data and then verifying the output
* _easier to refactor_ - changes can be planned and implemented in stages

### Empty Component

This component specifies an empty dataset. It is optionally possible to specify an encoding.

The list of fields supported is:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `empty` |
| `encodedAs` | no | An optional encoded specification |

Yaml example:
```yaml
# example of an encoded empty component 
emptyComponet:
  type: empty
  encodedAs: { value: STRING }
```

### Batch and Stream Component

In spark, it is possible to define an external source (like a file, or a kafka topic). 
This component allows for defining one of such sources. 

The list of fields supported is:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | One of: `batch`, `stream` |
| `format` | yes | One of the supported values supported by spark natively, lik e`parquet` or `json` |
| `options` | no | A map of values |
| `encodedAs` | no | An optional encoded specification |

Yaml examples:
```yaml
# parquet
parquetSource:
  type: batch
  format: parquet
  options:
    path: hdfs://...
    
# csv
csvSource:
  type: batch
  format: com.databricks.spark.csv
  options:
    inferSchema: true
    path: hdfs://...
```

Note that if a dataset need to be of streaming variety, then the type need to be `stream`. 
All the other fields are the same.

Yaml examples:
```yaml
# kafka
parquetSource:
  type: stream
  format: kafka
  options: { "kafka.bootstrap.servers": host1:port1,host2:port2, subscribe: topic1 }
```

### Inline Component

Sometimes it is useful to have a hardcoded dataset to inject into an execution plan. 
The inline component allow to do that, by reading a json like structure and parsing it using a user-provided schema.

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `inline` |
| `data` | no | A list of json rows. If not provided it will be an empty dataset |
| `schema` | no | An optional schema string (spark documentation). If undefined it will be inferred from the `data` field. |

Yaml example:
```yaml
# kafka
inlineComponend:
  type: inline
  data: 
    - { column1: "value1", column2: 10 }
    - { column1: "value2", column2: 20 }
  schema: "`column1` STRING,`column2` INT"
```

### Sql Component

Probably the most important component of all, the sql component allows to define a sql transformation on a possible set of inputs.
Inputs is a list of other components, that will be used as a list of predefined tables to be referenced by the sql in the component.

The list of fields supported is:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `sql` |
| `using` | no | An optional list of other components. These component swill be used as table names in the sql statement  |
| `sql` | yes | The sql statement |
| `udfs` | no | An optional list of user provided functions (see below) |
| `encodedAs` | no | An optional encoded specification |

Yaml example:
```yaml
source1:
  ...
source2:
  ...

sqlComponent:
  type: sql
  using: [ source1, source2 ]
  udfs:
    ofClasses:
      - sparkengine.plan.runtime.builder.dataset.TestUdf
  sql: >
    select 
      testUdf(t.value) as column 
    from 
      source1 as t 
    join 
      source2 as f 
    on 
      t.id = f.id"
```

It is also possible to have a sql generating data, without shaving to use any input component. In the following example the sql statements generates 2 rows:
```yaml
sqlComponentThatGeneratesData:
  type: sql
  sql: >
    select explode(array(
      named_struct("num", 0, "str", "a"), 
      named_struct("num", 1, "str", "b")
    )) as abc
```

#### UDF/UDAF Functions

The sql component can be extended by providing externally defined user defined function (UDF) or user defined aggregation functions (UDAF).
These function should extend the `dataengine.spark.sql.udf.SqlFunction` interface.
Utility classes are also available to help create udfs, they are `dataengine.spark.sql.udf.Udf` and `dataengine.spark.sql.udf.Udaf`.

A sql component needs a function library definition to use the user provided functions:
```yaml
# fragment that defines a list of udfs/udafs
sqlComponent:
  ...
  udfs:
    ofClasses:
      - sparkengine.plan.runtime.builder.dataset.TestStrUdf
      - sparkengine.plan.runtime.builder.dataset.TestIntUdf
```

A udf in Java may look like this:
```java
package sparkengine.plan.runtime.builder.dataset;

import dataengine.spark.sql.udf.Udf;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import javax.annotation.Nonnull;

public class TestIntUdf implements Udf {

    @Nonnull
    @Override
    public String getName() {
        return "testIntUdf";
    }

    @Nonnull
    @Override
    public DataType getReturnType() {
        return DataTypes.IntegerType;
    }

    @Override
    public UDF2<Integer, Integer, Integer> getUdf2() {
        return Integer::sum;
    }
}
```

### Encode Component

An encode components applies an encoding to its input. The input is specified using the `using` field.

The list of fields supported is:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `encode` |
| `encodedAs` | no | An optional encoded specification |

Yaml example:
```yaml
someComponent:
  ...

# example of an encoded component 
encodeComponet:
  type: encode
  using: someComponent
  encodedAs: { value: STRING }
```

### Transform Component

The most generic component of them all, the transform components allow for a specification of an external Java class to provide a user provided transformation.

The list of fields supported is:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `transform` |
| `using` | no | An optional list of other components to be passed as input to the transform component  |
| `transformWith` | yes | A Java fully qualified name of a class that specifies an implementation of the `dataengine.spark.transformation.DataTransformationN` interface |
| `encodedAs` | no | An optional encoded specification |

Yaml example:
```yaml
source1:
  type: inline
  data:
    - { key: "a", value: 1 }
    - { key: "a", value: 1 }
    - { key: "a", value: 1 }
    - { key: "b", value: 100 }
    - { key: "b", value: 200 }
    - { key: "c", value: 1 }

source2:
  type: inline
  data:
    - { key: "d", value: 1 }
    - { key: "d", value: 2 }
    - { key: "d", value: 3 }
    - { key: "a", value: 3 }

tx:
  type: transform
  using: [ source1, source2 ]
  transformWith: sparkengine.plan.runtime.builder.dataset.TestTransformation
```

Java code for transformation:
```java
package sparkengine.plan.runtime.builder.dataset;

import dataengine.spark.transformation.DataTransformationN;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class TestTransformation implements DataTransformationN<Row, Row> {

    @Override
    public Dataset<Row> apply(List<Dataset<Row>> datasets) {
        return datasets.stream().reduce(Dataset::union).orElseThrow(IllegalStateException::new);
    }

}
```

## Dataset Sinks

Once a dataset is ready to be used it can be consumed by a dataset consumer.
Just like a dataset component, a consumer can be described using a model, usually represented as yaml.
A dataset consumer is represented in Java as an extension of the `sparkengine.plan.model.sink.Sink` interface.

**TODO**

### Show Sink

For debugging purposes, a show sink can be used to print on the output terminal the head of the dataset.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `show` |
| `numRows` | no | Number of rows to show, defaults to 20.  |
| `truncate` | no | Number of chars for each column, defaults to 30.  |

Yaml Example:
```yaml
showSink:
  type: show
```

### View Sink

A view sink can be used to register the  in the dataset as a table in the catalog.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `collect` |
| `name` | yes | Name of the temporary view. |

Yaml Example:
```yaml
collectSink:
  type: collect
```

### Batch Sink

This sink describes a write operation of a dataset to the standard spark writer interface.
Refer to spark documentation for details.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `batch` |
| `format` | yes | Spark writer format (eg: `parquet`, `json`). |
| `mode` | no | Write mode. One of: `APPEND`, `OVERWRITE`, `ERROR_IF_EXISTS`, `IGNORE`. |
| `options` | no | A key-value map of options. Meaning is format-dependent |
| `partitionColumns` | no | A list of columns to be used to partition during save of data. |

Yaml Example:
```yaml
collectSink:
  type: batch
  format: parquet
  options: { path: 'abc' }
  mode: OVERWRITE
```

### Stream Sink

This sink describes a write operation of a streaming dataset to the standard spark writer interface.
Refer to spark documentation for details.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `batch` |
| `format` | yes | Spark writer format (eg: `kafka`). |
| `name` | yes | Name of the streaming query. |
| `mode` | no | Write mode. One of: `APPEND`, `COMPLETE`, `UPDATE`. |
| `checkpointLocation` | no | Checkpoint location for the stream (usually somewhere in hdfs or local filesystem). |
| `trigger` | no | Defines trigger for stream (see below). |
| `options` | no | A key-value map of options. Meaning is format-dependent |

Yaml Example:
```yaml
collectSink:
  type: stream
  name: queryNameHere
  format: kafka
  options: { path: 'abc' }
  trigger: { milliseconds: 60 }
  mode: APPEND
```

#### Triggers

A trigger defines the operation frequency of the stream. Without a trigger a micro-batch will be started as soon as previous bmicro batch is done.
In continuous mode spark operates using a single batch.
Refer to spark documentation for details.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | One of: `interval`, `once`, `continuous`. Defaults to `intervalMs`. |
| `milliseconds` | yes | This is only used for types `intervalMs`, `continuous`. |

Yaml Examples:
```yaml
# same
{ milliseconds: 60 }
{ type: interval, milliseconds: 1000 }

# once
{ type: once }

# continuous
{ type: continuous, milliseconds: 1000 }
```

### Foreach Sink

A streaming dataset can write its output in batch mode. 
This sink will wire each micro batch dataset to one or more pipelines described as a full fledged execution plan (see below).
Not ethat the execution plan will be fully reevaluated on every new micro batch dataset (ie: every trigger).

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `foreach` |
| `name` | yes | Name of the streaming query. |
| `mode` | no | Write mode. One of: `APPEND`, `COMPLETE`, `UPDATE`. |
| `trigger` | no | Defines trigger for stream (see below). |
| `options` | no | A key-value map of options. Meaning is format-dependent |
| `plan` | yes | An execution plan that will describe all the operations that should be executed on the micro batch dataset  |
| `batchComponentName` | yes | The name of the virtual component that can be referenced in the plan. This component will provide a dataset equal to the micro batch of the stream |

Yaml Examples:
```yaml
foreachSink:
  type: foreach
  name: queryName
  trigger: { milliseconds: 1000 }
  mode: APPEND
  batchComponentName: src
  plan:
    components:
      transformation1: { type: sql, using: src, sql: "select column from src" }
      transformation2: { type: encode, using: transformation1, encodedAs: { type: INT } }
    sinks:
      display: { type: show }
      fileOutput: { type: batch, format: parquet, options: { path: 'hdfs://...' }, mode: OVERWRITE }
    pipelines:
      - { source: src, sink: display }
      - { source: transformation2, sink: fileOutput }
```

## Execution Plan

As we stated before, an _execution plan_ is a set of datasets that can be routed to a set of consumers.
A single pair dataset/dataset consumer is called a _pipeline_, while the full set of all components/consumers/pipelines is an _execution plan_.
The model class that represents an execution plan is `sparkengine.plan.model.pipeline.Plan`.

In yaml term, an execution plan can be represented by a document divided in 3 parts: 
* a list of _components_ - this will describe datasets
* a list of _sinks_ - this will define dataset consumers
* a list of _pipelines_ - to pair a component with a sink

A high level example in yam;:
```yaml
components:
  source1:
    ...
  source2:
    ...
  component1:
    using: [ source1, source2 ]
    ...    
  component2:
    using: [ component1 ]
    ...

sinks:
  consumer1:
    ...
  consumer2:
    ...

pipelines:
  pipe1: { source: component1, sink: consumer1 }
  pipe2: { source: component2, sink: consumer2 }
```


