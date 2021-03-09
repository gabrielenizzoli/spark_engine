---
layout: default
parent: Components
nav_order: 8
---

# Transform Component

The most generic component of them all, the transform component, allow for a specification of an external Java class to provide a user provided transformation.
The provided class must extend `sparkengine.spark.transformation.DataTransformationN`.

If some parametrization is needed, the class `sparkengine.spark.transformation.DataTransformationWithParameters` is the right one.
In this case the `param` map in the component is properly serialized to the proper Java bean. 

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `transform` |
| `using` | no | An optional list of other components to be passed as input to the transform component  |
| `params` | yes (see details) | A map to provide parameters. Note that parameters are required for a transformation that supports parameters. |
| `transformWith` | yes | A Java fully qualified name of a class that specifies an implementation of the `dataengine.spark.transformation.DataTransformationN` interface |
| `encodedAs` | no | An optional encoded specification |

## Example (without parameters)

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

## Example (with parameters)

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

public class TestTransformationWithParams implements DataTransformationWithParameter<Row, Row, TestTransformationWithParams.Params> {

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
