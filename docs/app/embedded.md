---
layout: default
title: Embedded runner
parent: Application
nav_order: 2
---

# The embedded runner

If a spark session is already available (ie: zeppelin, jupyterhub + almond. your own code), then is it possible to just execute the plan programmatically:

```java
import sparkengine.plan.app.runner.PlanRunner;
import sparkengine.plan.app.runner.RuntimeArgs;

var args = RuntimeArgs
        .builder()
        .planLocation("/somewhere/yourPlan.yaml")
        .build();

PlanRunner.builder()
        .sparkSession(sparkSession)
        .runtimeArgs(args)
        .build()
        .run();
```
