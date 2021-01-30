---
sort: 2
---

# Trigger Field

A trigger defines the operation frequency of a streaming sink. 
Without a trigger a micro-batch will be started as soon as previous micro batch is done.
In _continuous mode_ spark operates using a single batch.
Refer to spark documentation for details.

## Fields

| Field | Required | Possible Value |
| ----- | -------- | -------------- |
| `type` | yes | One of: `interval`, `once`, `continuous`. Defaults to `intervalMs`. |
| `milliseconds` | yes | This is only used for types `intervalMs`, `continuous`. |

## Examples

Yaml Examples:
```yaml
# same
{ milliseconds: 60 }
{ type: interval, milliseconds: 1000 }

# once
{ type: once }

# continuous
{ type: continuous, milliseconds: 1000 }
```