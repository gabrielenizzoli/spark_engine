---
sort: 4
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
encodeComponet:
  type: encode
  using: someComponent
  encodedAs: { value: STRING }
```
