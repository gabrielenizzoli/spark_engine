---
sort: 1
---

# What is a Sink

As we described before, a dataset is described by on eor more components.
Once a dataset is defined, it needs to be consumed to do something.
A consumer describes an action on a dataset (ie: save to disk, show to terminal, etc etc).
For this reason, a _sink_ model abstraction is also provided to describe dataset consumers.

Just like a dataset component, a consumer can be described using a model, usually represented as yaml.
A dataset consumer is represented in Java as an extension of the `sparkengine.plan.model.sink.Sink` interface.
