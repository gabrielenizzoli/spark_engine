---
sort: 3
---

# The plan-model-builder module

This module exposes a set of utilities to deserialize a representation of a plan model to its Java counterpart.
Currently only the YAML deserializer is available in the `sparkengine.plan.model.builder.ModelFactory` class.

## Build a Plan from a YAML source

Example:
```java
import sparkengine.plan.model.builder.*;

InputStreamSupplier inputStream = () -> return some inputStream that reads a yaml;

var plan = ModelFactories.readPlanFromYaml(inputStream);
```