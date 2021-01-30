---
sort: 3
---

### Show Sink

For debugging purposes, a show sink can be used to print on the output terminal the head of the dataset.

List of fields:

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | `show` |
| `numRows` | no | Number of rows to show, defaults to 20.  |
| `truncate` | no | Number of chars for each column, defaults to 30.  |

Yaml Example:
```yaml
showSink:
  type: show
```
