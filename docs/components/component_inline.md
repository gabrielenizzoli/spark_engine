---
layout: default
parent: Components
nav_order: 6
---

# Inline Component

The inline component is an inline dataset where the data is specified directly into the plan.
The data is provided as a json-like structure.
It can be use for testing or for configuration.

## Parameters

An inline dataset can have some of its data replaced by command like parameters or environment variables.
If a value in the dataset is a name enclosed between a `${` and a `}` the it will be replaced by a provided parameter.
If a replacement is not present, an error will be thrown.
Parameter can be passed on the command line at start time and can be optionally read from the environment.

As an example, in the following component the `${NAME}` string will be replaced by the value of the `NAME` parameter.

```yaml
# kafka
inlineComponent:
  type: inline
  data: 
    - { name: "${COLUMN_NAME}" }
```

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `inline` |
| `data` | no | A list of json rows. If not provided it will be an empty dataset |
| `schema` | no | An optional schema string (spark documentation). If undefined it will be inferred from the `data` field. |

## Examples

Yaml example:

```yaml
# kafka
inlineComponent:
  type: inline
  data: 
    - { column1: "value1", column2: 10 }
    - { column1: "value2", column2: 20 }
  schema: "`column1` STRING,`column2` INT"
```
