---
layout: default
parent: Components
nav_order: 6
---

# Inline Component

The inline component is an inline dataset where the data is specified directly into the plan.
The data is provided as a json-like structure.
It can be use for testing or for configuration.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `inline` |
| `data` | no | A list of json rows. If not provided it will be an empty dataset |
| `schema` | no | An optional schema string (spark documentation). If undefined it will be inferred from the `data` field. |

## Examples

Yaml example:

```yaml
inlineComponent:
  type: inline
  data: 
    - { column1: "value1", column2: 10 }
    - { column1: "value2", column2: 20 }
  schema: "`column1` STRING,`column2` INT"
```
