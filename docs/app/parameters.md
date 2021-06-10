---
layout: default
title: Parameters and Templating
parent: Application
nav_order: 3
---

# Parameters

The spark engine supports parameters replacement in some fields of the execution plan.
Parameters can be defined on the command line at start time and can be optionally enriched with the environment.

## Different kind of replacements

There are two main kind of replacement, the first one being a typed one, teh second one a template one.
Note that if a replacement is not present and no default value is provided, an error will be thrown.

### Typed Parameters

A typed parameter is a string that will be replaced by a parameter and optionally converted to a boolean or a number.
Note that a typed parameter only contains a single replacement.

In its more generic form, a typed parameter replacement is a string that looks like `${type:name:default value}`, where:

* `type` is optional and can be `s` (for string type), `b` (boolean), or `n` (for numeric type); the type is used to convert the parameter to the right type in the inline component;
* `name` is the expected parameter name to use to replace in the component;
* `default value` is an optional non-null or non-empty value to use if a parameter is not provided.

### Templates

A template string is a string where every substring enclosed within a `${` and a `}` is replaced by the respective parameter.

In a template there might be zero or more replacements.
In its more generic form, a parameter replacement in the template string looks like `${name:default value}`, where:

* `name` is the expected parameter name to use to replace in the component;
* `default value` is an optional non-null or non-empty value to use if a parameter is not provided.

## Replacements point

### Replacements for Inline Components

Any value in the inline component dataset is a candidate for a replacement parameter.
It is both a typed replacement and a template.

As an example, in the following component the `${n:NAME:100}` string will be replaced by the value of the `NAME` parameter, after converting it to a number.
On the contrary the `col2` field is a template and will replace `${TEMPLATE:template}` inside the string with the parameter called `TEMPLATE`.

```yaml
inlineComponent:
  type: inline
  data:
    - { col: "${n:NAME:100}", col2: "this is a ${TEMPLATE:template}" }
```

### Replacements for the Batch/Stream Component and Batch/Stream Sink

Any option in the batch/stream component is a template.
As an example, in the following component the `${b:INFER:true}` string will be replaced by the value of the `INFER` parameter, after converting it to a boolean.

```yaml
csvSource:
  type: batch
  format: com.databricks.spark.csv
  options:
    inferSchema: ${INFER:true}
    path: hdfs://...
```