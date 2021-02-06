---
sort: 9
---

# Schema test Component

This component provides a facility to validate if a dataset schema matches a proper schema.
The schema is expressed as DDL (given a Spark `StructType`, the schema is extracted by calling the `toDDL` method).
More documentation [can be found here](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/types/StructType.html#toDDL:String).

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `schemaTest` |
| `using` | yes | An reference to another component to be passed as input to the transform component  |
| `schema` | yes | A string that defines the schema that the dataset is tested against. A failure to pass the schema test wil result in an exception.  |

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
  type: schemaTest
  using: source
  schema: "`key` STRING,`value` INT"
```
