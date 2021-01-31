---
sort: 9
---

# Map Component

The map components allow for a specification of an external Java class to transform a single dataset.
The provided class must extend `sparkengine.spark.transformation.DataTransformation`.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `transform` |
| `using` | no | An optional reference to another component to be passed as input to the transform component  |
| `transformWith` | yes | A Java fully qualified name of a class that specifies an implementation of the `dataengine.spark.transformation.DataTransformation` interface |
| `encodedAs` | no | An optional encoded specification |

## Examples

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
  type: transform
  using: source1
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
