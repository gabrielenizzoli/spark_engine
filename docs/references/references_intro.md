---
layout: default
parent: References
nav_order: 1
---

# The Reference Component/Sink

A special type of component and sink, the reference type, can be used to externalize part of the plan to separate files.
At his core, this component/sink can be used to tell the spark engine application to fetch components/sinks somewhere else.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `ref` |
| `mode` | no | Resolution mode for the resource. Can be `RELATIVE` or `ABSOLUTE`. Defaults to `RELATIVE`. |
| `ref` | no (yes if `ABSOLUTE` resolution mode) | Location where to look for the resource. Resolution rules are defined below. |

## Absolute Resolution rules

If the resolution mode is set to `ABSOLUTE`, then the `ref` field must be specified, and it will be used to locate the resource.
The value in this case is usually a fully qualified url.

Example:

```yaml
components:
  input: { type: ref, mode: ABSOLUTE, ref: http://server/component.yaml }
sinks: 
  ouput: { type: ref, mode: ABSOLUTE, ref: hdfs://somewhere/sink.yaml }
pipelines:
  pipe: { component: input, sink: ouput }
```

## Relative Resolution rules (without no `ref` field)

If resolution mode is set to `RELATIVE` and the `ref` field is _NOT_ specified, components and sinks are resolved by following the _path_ to the component.
Note that the `RELATIVE` resolution mode is the default, and does not need to be specified in the model description

For instance, assuming a plan is located at `http://server/path/planName.yaml`:

```yaml
components:
  input: { type: ref }
sinks: 
  ouput: { type: ref }
pipelines:
  pipe: { component: input, sink: ouput }
```

The 2 external resources are located at:

* `http://server/path/planName_components_input.yaml`
* `http://server/path/planName_sinks_output.yaml`

In the first case (the component called `input`) the path is composed by:

* `planName`: the name of the plan in the original url
* `components`: the components group
* `input`: the name of the component

More in general, given a plan located at url `urlRoot/planName.yaml`, a relative resource will be located at a url `urlRoot/planName_<parts>.yaml`, where parts area list of strings joined by the `_` character.

The list of parts is composed such that:

* it starts with the `components` string if the part is a reference to a component
* it starts with the `sinks` string if the part is a reference to a sink
* it has the list of all the components or sinks names required to reach the resource

## Relative Resolution rules (with the `ref` field)

When the resolution mode is `RELATIVE` and the `ref` field is specified, the location of the resources is simply the name of the plan and the value of the ref field.

For instance, assuming a plan is located at `http://server/path/planName.yaml`:

```yaml
components:
  input: { type: ref, ref: component1 }
sinks: 
  ouput: { type: ref, ref: sink2 }
pipelines:
  pipe: { component: input, sink: ouput }
```

The 2 external resources are located at:

* `http://server/path/planName_component1.yaml`
* `http://server/path/planName_sink2.yaml`
