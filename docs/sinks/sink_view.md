---
sort: 4
---

### View Sink

A view sink can be used to register the  in the dataset as a table in the catalog.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `collect` |
| `name` | yes | Name of the temporary view. |

Yaml Example:
```yaml
collectSink:
  type: collect
```
