---
sort: 3
---

## Empty Component

This component specifies an empty dataset. It is optionally possible to specify an encoding.

The list of fields supported is:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `empty` |
| `encodedAs` | no | An optional encoded specification |

Yaml example:
```yaml
# example of an encoded empty component 
emptyComponet:
  type: empty
  encodedAs: { value: STRING }