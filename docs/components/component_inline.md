---
sort: 6
---

## Inline Component

Sometimes it is useful to have a hardcoded dataset to inject into an execution plan.
The inline component allow to do that, by reading a json like structure and parsing it using a user-provided schema.

### Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `inline` |
| `data` | no | A list of json rows. If not provided it will be an empty dataset |
| `schema` | no | An optional schema string (spark documentation). If undefined it will be inferred from the `data` field. |

### Examples

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
