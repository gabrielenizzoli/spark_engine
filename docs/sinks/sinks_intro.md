---
layout: default
parent: Sinks
nav_order: 1
---

# What is a Sink

As [we said before](/components), a dataset is defined by one or more components.
Once a dataset is created, it needs to be consumed for something to happen.
A **consumer** describes an action on a dataset (ie: save to disk, show to terminal, etc etc).
For this reason, a **sink** model abstraction is also provided to describe dataset consumers.

Just like a dataset component, a dataset consumer can be described using a model, usually represented as yaml.
A dataset consumer is represented in Java as an extension of the `sparkengine.plan.model.sink.Sink` interface.
