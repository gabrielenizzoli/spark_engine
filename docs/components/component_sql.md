---
layout: default
parent: Components
nav_order: 7
---

# Sql Component

{: .no_toc }

The sql component defines a sql transformation on a possible set of inputs.
{: .fs-6 }

## Table of contents

{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `sql` |
| `using` | no | An optional list of other component's names. These components will be used as table names in the sql statement. If this field is not present, the list of components will be extracted from the sql statement itself.  |
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
      t.id = f.id
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
      - className: sparkengine.plan.runtime.builder.dataset.TestIntUdf
```

### Java UDF/UDAF

In a library it is possible to define Java-coded udfs.
These function should extend the `dataengine.spark.sql.udf.SqlFunction` interface.
Utility interfaces are also available to help create udfs, they are `dataengine.spark.sql.udf.UdfDefinition` and `dataengine.spark.sql.udf.UdafDefinition`.

Fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | no | If not provided, defaults to `java` |
| `className` | yes | The java class that implements the udf. |
| `accumulators` | no | A mapping between how an accumulator is used internally and how it is named globally. |  

A udf in Java may look like this:

```java
package my.pack.name;

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

Yaml example:

```yaml
sqlComponent:
  type: sql
  udfs:
    list:
      - { className: my.pack.name.TestIntUdf }
  sql: >
    select testUdf(source.value) as column 
    from source
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

Fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `scala` |
| `name` | yes | The name of the udf. |
| `scala` | yes | The code of the udf. |
| `accumulators` | no | A mapping between how an accumulator is used internally and how it is named globally. |

### UDF Context and Accumulators

The **udf context** is an object that can be injected in any udf to provide additional functionalities (like accumulators).
The context is defined by `sparkengine.spark.sql.udf.UdfContext` and is injected before registering the udf in the logical plan.
An udf that implements `sparkengine.spark.sql.udf.UdfWithContext` will receive the udf context objet before registration.

In the context, accumulators are available using the `acc` method as follows: `acc(<accumulator name>, <increment value>)`.
In this method call:

* `<accumulator name>`: name of the accumulator
* `<increment value>`: how much the accumulator should be incremented

Note that the accumulator name used internally in the udf is not the global name of the accumulator.
This mapping is specified in the `accumulators` map of the udf definition.
This way the same udf can be used in multiple places, and the global accumulator name reported to  the metric system may be different.
As an example, in the following the named used in the udf is `internal.name`, but for metric reporting purpose the name is `global.name`:

```yaml
sqlComponent:
  sql: select testIntUdf(value) from source
  udfs:
    list:
      - type: scala
        name: testIntUdf
        accumulators: { 'internal.name': 'global.name' }
        scala: >
          (in:Int) => { ctx.acc("internal.name", 1); in*2 }
```

NOTE: if an accumulator name that is not mapped is incremented, a generic fallback accumulator will be used to collect the increment.
