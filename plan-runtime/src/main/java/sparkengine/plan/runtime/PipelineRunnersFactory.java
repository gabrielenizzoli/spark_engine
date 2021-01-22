package sparkengine.plan.runtime;

import java.util.List;

public interface PipelineRunnersFactory {

    List<PipelineName> getPipelineNames();

    PipelineRunner buildPipelineRunner(PipelineName pipelineName) throws PipelineRunnersFactoryException;

}
