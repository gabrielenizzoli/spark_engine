---
layout: default
parent: Components
nav_order: 7
---

# Sql Component

Probably the most important component of all, the sql component allows to define a sql transformation on a possible set of inputs.
Inputs is a list of other components, that will be used as a list of predefined tables to be referenced by the sql in the component.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `sql` |
| `using` | no | An optional list of other components. These component swill be used as table names in the sql statement  |
| `sql` | yes | The sql statement |
| `udfs` | no | An optional list of user provided functions (see below) |
| `encodedAs` | no | An optional encoded specification |

## Examples

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

## UDF/UDAF Functions

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