---
layout: default
parent: Components
nav_order: 8
---

# Transform Component
{: .no_toc}

The transform component allow for an external Java class to define a user provoded dataset transformation.
{: .fs-6 }

## Table of contents
{: .no_toc .text-delta }

- TOC
{:toc}

---

## Introduction

There are two flavors of the transform component, depending on the number of inputs:

* if the expected number of inputs is exactly 1, the the component os of type `map` and the expected Java class specified must implement `sparkengine.spark.transformation.DataTransformation`;
* if the expected number of inputs is variable, the the component os of type `transform` and the expected Java class specified must implement `sparkengine.spark.transformation.DataTransformationN`.

If some parametrization is needed, the provided class must also implement `sparkengine.spark.transformation.DataTransformationWithParameters`.
In this case the `param` map in the component is properly serialized to the requested Java bean.

Finally, if some additional framework facilities are needed (like accumulators), the transformation can extend `sparkengine.spark.transformation.context.DataTransformationWithContext`.
In this case the transformation will be injected a `Broadcast<DataTransformationContext>` object via the `setTransformationContext(...)` method.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `map` (exactly one source) or `transform` (variable number of input sources) |
| `using` | no | An optional list of other components to be used as input to the transform component  |
| `params` | no (see details) | A map that provides parameters. Note that parameters are required for a transformation that implements `sparkengine.spark.transformation.DataTransformationWithParameters`. |
| `transformWith` | yes | A Java fully qualified name of a class that specifies an implementation of the `dataengine.spark.transformation.DataTransformation` or `dataengine.spark.transformation.DataTransformationN` interface. |
| `accumulators` | no | A mapping between how an accumulator is used internally and how it is named globally. |
| `encodedAs` | no | An optional encoded specification. |

## Single-input Transformation (`map`)

### Examples

Yaml example:

```yaml
source:
  type: inline
  data:
    - { key: "a", value: 1 }
    - { key: "a", value: 1 }
    - { key: "a", value: 1 }
    - { key: "b", value: 100 }
    - { key: "b", value: 200 }
    - { key: "c", value: 1 }

tx:
  type: map
  using: source
  transformWith: sparkengine.plan.runtime.builder.dataset.MapTransformation
```

Java code for transformation:

```java
package sparkengine.plan.runtime.builder.dataset;

import dataengine.spark.transformation.DataTransformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class MapTransformation implements DataTransformation<Row, Row> {

    @Override
    public Dataset<Row> apply(Dataset<Row> dataset) {
        return dataset.cache();
    }

}
```

## Multi-input Transformation (`transform`)

### Example (without parameters)

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

### Example (with parameters)

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

txWithParams:
  type: transform
  using: [ source1, source2 ]
  params: { flag: true, value: "newCol" }
  transformWith: sparkengine.plan.runtime.builder.TestTransformationWithParams
```

Java code for transformation:

```java
package sparkengine.plan.runtime.builder;

import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import sparkengine.spark.transformation.DataTransformationN;
import sparkengine.spark.transformation.DataTransformationWithParameters;

import javax.annotation.Nullable;
import java.util.List;

public class TestTransformationWithParams implements DataTransformationN<Row, Row>, DataTransformationWithParameter<TestTransformationWithParams.Params> {

    private Params params;

    @Data
    public static class Params {
        boolean flag;
        String value;
    }

    @Override
    public Dataset<Row> apply(List<Dataset<Row>> datasets) {
        return datasets.stream().reduce(Dataset::union)
                .map(ds -> ds.withColumn(params.getValue(), functions.lit(params.isFlag())))
                .orElseThrow(IllegalStateException::new);
    }

    @Override
    public Class<Params> getParameterType() {
        return Params.class;
    }

    @Override
    public void setParameter(@Nullable Params parameter) {
        this.params = parameter;
    }

}
```
