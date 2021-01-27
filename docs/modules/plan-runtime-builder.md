---
sort: 5
---

# The plan-runtime-builder module

The next step is to create a runtime entity that can execute all the pipelines. 
As we discussed in the _pipeline-runtime_ module, the runtime entity that will provide this facility is the `PlanFactory`.
THis factory can be instantiated by a plan object.

## Build PipelineRunner from a Plan

The `PlanFactory` interface looks like this:
```java
public interface PlanFactory {

    List<PipelineName> getPipelineNames();

    PipelineRunner buildPipelineRunner(PipelineName pipelineName) throws PlanFactoryException;

}
```

This factory provides:
1) a list of pipeline names
2) a way to create a runner given a pipeline name 

As simple use case in which pipelines are created and executed sequentially is:
```java
for (var pipelineName : pipelineRunnersFactory.getPipelineNames()){
    var pipelineRunner = pipelineRunnersFactory.buildPipelineRunner(pipelineName);
    pipelineRunner.run();
}
```

To create a pipeline factory we use the `ModelPlanFactory` class:
```java
import sparkengine.plan.model.Plan;
import sparkengine.plan.runtime.builder.ModelPipelineRunnersFactory;

var plan = ...;
var pipelineRunnersFactory = ModelPlanFactory.ofPlan(sprkSession, plan);
```
