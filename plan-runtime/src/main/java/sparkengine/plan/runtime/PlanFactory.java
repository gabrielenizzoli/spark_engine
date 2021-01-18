package sparkengine.plan.runtime;

import java.util.List;

public interface PlanFactory {

    List<PipelineName> getPipelineNames();

    PipelineRunner buildPipelineRunner(PipelineName pipelineName) throws PlanFactoryException;

}
