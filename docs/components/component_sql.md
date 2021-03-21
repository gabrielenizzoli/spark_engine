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
    list:
      - { className: sparkengine.plan.runtime.builder.dataset.TestUdf }
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

The sql component can be extended by providing user defined function (UDF) or user defined aggregation functions (UDAF).

A sql component has a section where udf can be defined.
The access to the udf is scoped by component, so only sql in the component where the udfs are defined will have access to them.
The udfs are defined in a function library definition. The most common library definition is the `list` type:

```yaml
sqlComponent:
  sql: select testIntUdf(value) from source
  udfs:
    list:
      - { className: sparkengine.plan.runtime.builder.dataset.TestIntUdf }
```

### Java UDF/UDAF

In a library it is possible to define Java-coded udfs.
These function should extend the `dataengine.spark.sql.udf.SqlFunction` interface.
Utility interfaces are also available to help create udfs, they are `dataengine.spark.sql.udf.UdfDefinition` and `dataengine.spark.sql.udf.UdafDefinition`.

A udf in Java may look like this:
```java
package sparkengine.plan.runtime.builder.dataset;

import dataengine.spark.sql.udf.UdfDefinition;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import javax.annotation.Nonnull;

public class TestIntUdf implements UdfDefinition {

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
    public UDF2<Integer, Integer, Integer> asUdf2() {
        return Integer::sum;
    }
}
```

### Scala UDF

A quicker way to provide an udf is to script it in java.
An example is:
```yaml
sqlComponent:
  sql: select testIntUdf(value) from source
  udfs:
    list:
      - { type: scala, name: testIntUdf, scala: "(in:Int) => in*2" }
```

The scala scriptlet may contain any kind of code, but note that the scala scriptlet must evaluate to a `Function*` for the script fragment to be valid.  

### UDF Context

The **udf context** is an object that can be injected in any udf to provide additional functionalities (like accumulators).
The context is defined by `sparkengine.spark.sql.udf.UdfContext` and is injected before registering the udf in the logical plan.
An udf that implements the `sparkengine.spark.sql.udf.UdfWithContext` can receive the udf context objet.
