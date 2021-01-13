---
sort: 3
---

# The pipeline-model module

This module exposes a model abstraction to describe a dataset. 
The model abstraction can be usually expressed in a common format, usually yaml (or json), that the code is able to translate in a set of Java beans.

Additionally, a model abstraction is also provided to describe dataset consumers. A consumer describes an action on a dataset (ie: save to disk, show to terminal, etc etc).

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

In Java terms, the root of the component hierarchy is the `Component` interface. Every component extends form it.
The common characteristic of a component is to carry all the data needed to produce or transform a dataset described by another component.
By properly chaining many components it is thus possible to compose a complex dataset.

In an execution plan many components work together to provide information about how to build mon eor more datasets.
Some components provide information on how to generate datasets from external sources (like the _batch_ component), while others make use of an existing dataset and transforms it (like the _encode_ component).
Each component is associated to a unique name in an execution plan. This way it will be easy to reference different components. 

In the following yaml execution plan a `operation` components uses as an input the dataset provided by two other components:
```yaml
source1:
  ...

source2:
  ...

operation:
  using: [ source1, source2 ]
  ...
```

An execution plan is _consistent_ if all the component referenced are available and if there are no circular references (ie: the graph of components must be an acyclic graph).
By referencing a component name in a consistent execution plan, all the information to generate a dataset is available.
In the example above, if the plan is consistent, the `operation` components should carry all the information required to generate a dataset, once the datasets from the component `source1` and `source2` are generated.

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
      - dataengine.pipeline.runtime.builder.dataset.TestUdf
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
      - dataengine.pipeline.runtime.builder.dataset.TestStrUdf
      - dataengine.pipeline.runtime.builder.dataset.TestIntUdf
```

A udf in Java may look like this:
```java
package dataengine.pipeline.runtime.builder.dataset;

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

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `transform` |
| `using` | no | An optional list of other components to be passed as input to the transform component  |
| `transformWith` | yes | A Java fully qualified name of a class that specifies an implementation of the `dataengine.spark.transformation.DataTransformationN` interface |
| `encodedAs` | no | An optional encoded specification |

**TODO: yaml example**

## Dataset Consumers

**TODO: consumer docs here**