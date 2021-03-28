---
layout: default
parent: Components
nav_order: 4
---

# Encode Component

A encode components applies an encoding to its input. The input is specified using the `using` field.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `encode` |
| `encodedAs` | no | An optional encoded specification |

## Examples

Yaml example:

  ```yaml
someComponent:
  ...

# example of an encoded component 
encodeComponent:
  type: encode
  using: someComponent
  encodedAs: { value: STRING }
```
