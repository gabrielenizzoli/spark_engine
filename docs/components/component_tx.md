---
sort: 8
---

# Transform Component

The most generic component of them all, the transform components allow for a specification of an external Java class to provide a user provided transformation.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `transform` |
| `using` | no | An optional list of other components to be passed as input to the transform component  |
| `transformWith` | yes | A Java fully qualified name of a class that specifies an implementation of the `dataengine.spark.transformation.DataTransformationN` interface |
| `encodedAs` | no | An optional encoded specification |

## Examples

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
