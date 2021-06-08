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

An inline dataset can have some of its data replaced by command-line parameters or environment variables.
If a value in the dataset is a string whose value is enclosed between a `${` and a `}`, its value will be replaced by teh respective value in the parameters map.
If a replacement is not present and no default value is provided, an error will be thrown.
Parameter can be passed on the command line at start-time and can be optionally read from the environment.

In its more generic form, a parameter replacement looks like `${type:name:default value}`, where:

* `type` is optional and can be `s` (for string type), `b` (boolean), or `n` (for numeric type); the type is used to convert the parameter to the right type in the inline component;
* `name` is the expected parameter name to use to replace in the component;
* `default value` is an optional non-null or non-empty value to use if a parameter is not provided.  

As an example, in the following component the `${n:NAME:100}` string will be replaced by the value of the `NAME` parameter, after converting it to a number.

```yaml
inlineComponent:
  type: inline
  data: 
    - { col: "${n:NAME:100}" }
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
inlineComponent:
  type: inline
  data: 
    - { column1: "value1", column2: 10 }
    - { column1: "value2", column2: 20 }
  schema: "`column1` STRING,`column2` INT"
```
