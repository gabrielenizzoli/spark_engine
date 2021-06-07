---
layout: default
title: Embedded runner
parent: Application
nav_order: 2
---

# The embedded runner

If a spark session is already available (ie: zeppelin, jupyterhub + almond, your own code, etc etc), then is it possible to just execute the plan programmatically:

```java
import sparkengine.plan.app.runner.PlanRunner;
import sparkengine.plan.app.runner.RuntimeArgs;

RuntimeArgs runtimeArgs = ...

PlanRunner.builder()
        .sparkSession(sparkSession)
        .planLocation("/somewhere/yourPlan.yaml")
        .runtimeArgs(runtimeArgs)
        .build()
        .run();
```

Optionally arguments can be specified in the `runtimeArgs` class if something different than the default is required.