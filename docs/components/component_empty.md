---
sort: 3
---

## Empty Component

This component specifies an empty dataset. It is optionally possible to specify an encoding.

### Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `empty` |
| `encodedAs` | no | An optional encoded specification |

### Examples

Yaml example:
```yaml
# example of an encoded empty component 
emptyComponet:
  type: empty
  encodedAs: { value: STRING }
```