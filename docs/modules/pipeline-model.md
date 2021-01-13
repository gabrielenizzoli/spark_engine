---
sort: 3
---

# The pipeline-model module

In order to describe a dataset, and a dataset consumer we will use a model abstraction. 
This model role is to properly break up how we define how a dataset is ultimately created and composed.

## Utilities

### Dataset Encoder

Datasets in spark can be encoded (something similar to a strong typing). 
An encoder can me described in the model as a property to many components. 
The encoder will be applied to the output. 
Available encoders in the model are `value`, `tuple`, `bean` and `seralization`.

| Encoder Type | Possible Value |
| ------------ | -------------- |
| `value` | One of: `BINARY`, `BOOLEAN`, `BYTE`, `DATE`, `DECIMAL`, `DOUBLE`, `FLOAT`, `INSTANT`, `INT`, `LOCALDATE`, `LONG`, `SHORT`, `STRING`, `TIMESTAMP` |
| `tuple` | A list of 2 or more encoders |
| `bean` | The fully qualified name of a a Java class to represent the schema and type of all fields |
| `serialization` | One of: `JAVA`, `KRYO`; plus a class name. |

Some example of an encoder in yaml representation is:
```yaml
# value
encodedAs: { schema: value, value: STRING }

# tuple
encodedAs: { schema: tuple, of: [ { schema: value, value: STRING }, { schema: value, value: INT } ] }

# bean
encodedAs: { schema: bean, ofClass: some.java.class.Bean }

# serialization
encodedAs: { schema: serialization, variant: KRYO , ofClass: some.java.class.Bean }
```

Note that the default serialization is `value`, so it can be omitted:
```yaml
# same
encodedAs.value: STRING
encodedAs: { schema: value, value: STRING }

# same
encodedAs: { schema: tuple, of: [ { value: STRING }, { value: INT } ] }
encodedAs: { schema: tuple, of: [ { schema: value, value: STRING }, { schema: value, value: INT } ] }
```

### UDF/UDAF Functions

The sql component can use externally defined user defined function (UDF) or user defined aggregation functions (UDAF).
These function should extend the `dataengine.spark.sql.udf.SqlFunction` interface. 
Utility classes are also available to help create udfs, they are `dataengine.spark.sql.udf.Udf` and `dataengine.spark.sql.udf.Udaf`.

A sql component needs a function library definition to use the user provided functions. A yaml example:
```yaml
# fragment that defines a list of udfs/udafs
sqlComponent:
  ...
  udfs:
    ofClasses:
      - dataengine.pipeline.runtime.builder.dataset.TestStrUdf
      - dataengine.pipeline.runtime.builder.dataset.TestIntUdf
```


## Dataset Components

In Java terms, the root of the hierarchy is the `Component` interface. Every component extends form it.
**TODO: EXPLAIN COMPOSITION**

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
| `udfs` | no | An optional list of user provided functions |
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
  sql: select explode(array(named_struct("num", 0, "str", "a"), named_struct("num", 1, "str", "b"))) as abc
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